# scripts/bootstrap-data-tools.ps1
# Creates ../dbt and ../great_expectations scaffolding to match volume mounts.

param()

$ErrorActionPreference = 'Stop'

function Write-Text($Path, $Content) {
  $dir = Split-Path -Parent $Path
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
  Set-Content -Path $Path -Value $Content -Encoding UTF8
}

$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Resolve-Path (Join-Path $Here '..')
$DbtRoot = Join-Path $Root 'dbt'
$GeRoot  = Join-Path $Root 'great_expectations'

# ----- dbt scaffold -----
New-Item -ItemType Directory -Path $DbtRoot -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $DbtRoot 'models\staging') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $DbtRoot 'models\marts') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $DbtRoot 'seeds') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $DbtRoot 'profiles') -Force | Out-Null

$dbt_project = @'
name: olist_dbt
version: 1.0.0
config-version: 2

profile: olist

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  olist_dbt:
    +materialized: view
    staging:
      +schema: staging
    marts:
      +schema: marts
'@

$profiles_yml = @'
olist:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres-warehouse
      user: warehouse
      password: warehouse
      port: 5432
      dbname: warehouse
      schema: analytics
      threads: 4
'@

$packages_yml = @'
packages: []
'@

$seed_orders = @'
order_id,order_status,order_purchase_timestamp
1,delivered,2025-01-01 10:00:00
2,shipped,2025-01-02 12:30:00
3,canceled,2025-01-03 09:15:00
'@

$stg_orders_sql = @'
select
  cast(order_id as integer) as order_id,
  order_status,
  cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp
from {{ ref('orders') }}
'@

$stg_schema_yml = @'
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: order_status
        tests:
          - not_null
'@

$marts_orders_summary_sql = @'
with src as (
  select * from {{ ref('stg_orders') }}
)
select
  order_status,
  count(*)::int as cnt
from src
group by order_status
order by order_status
'@

$marts_schema_yml = @'
version: 2

models:
  - name: orders_summary
    columns:
      - name: order_status
        tests:
          - not_null
      - name: cnt
        tests:
          - not_null
'@

$run_dbt_ps1 = @'
param()

# Runs dbt inside the Airflow webserver container using the mounted /opt/airflow/dbt folder.
docker compose exec airflow-webserver bash -lc `
  "cd /opt/airflow/dbt && dbt --no-use-colors deps && dbt seed --profiles-dir profiles && dbt run --profiles-dir profiles && dbt test --profiles-dir profiles"
'@

Write-Text (Join-Path $DbtRoot 'dbt_project.yml') $dbt_project
Write-Text (Join-Path $DbtRoot 'packages.yml') $packages_yml
Write-Text (Join-Path $DbtRoot 'profiles\profiles.yml') $profiles_yml
Write-Text (Join-Path $DbtRoot 'seeds\orders.csv') $seed_orders
Write-Text (Join-Path $DbtRoot 'models\staging\stg_orders.sql') $stg_orders_sql
Write-Text (Join-Path $DbtRoot 'models\staging\schema.yml') $stg_schema_yml
Write-Text (Join-Path $DbtRoot 'models\marts\orders_summary.sql') $marts_orders_summary_sql
Write-Text (Join-Path $DbtRoot 'models\marts\schema.yml') $marts_schema_yml
Write-Text (Join-Path $DbtRoot 'run_dbt.ps1') $run_dbt_ps1

# ----- Great Expectations scaffold -----
New-Item -ItemType Directory -Path $GeRoot -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $GeRoot 'uncommitted\validations') -Force | Out-Null

$ge_requirements = @'
great_expectations>=0.18.0
pandas
sqlalchemy
psycopg2-binary
'@

$ge_runner = @'
import os
import json
import pandas as pd
from sqlalchemy import create_engine

try:
    import great_expectations as ge
except Exception as e:
    raise SystemExit("great_expectations is not installed inside the container. Install it or bake it into the image.") from e

# Use warehouse Postgres by default (matches docker-compose services)
conn = os.getenv("GE_WAREHOUSE_CONN", "postgresql+psycopg2://warehouse:warehouse@postgres-warehouse:5432/warehouse")
engine = create_engine(conn)

# Validate the dbt mart: analytics.orders_summary
df = pd.read_sql("select * from analytics.orders_summary", con=engine)

dataset = ge.from_pandas(df)
dataset.expect_column_values_to_not_be_null("order_status")
dataset.expect_column_values_to_be_in_set("order_status", ["delivered", "shipped", "canceled"])
dataset.expect_column_values_to_not_be_null("cnt")
dataset.expect_column_values_to_be_between("cnt", min_value=1)

result = dataset.validate()

out_dir = os.path.join(os.path.dirname(__file__), "uncommitted", "validations")
os.makedirs(out_dir, exist_ok=True)
out_file = os.path.join(out_dir, "orders_summary_validation.json")
with open(out_file, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, default=str)

print("Validation success:", result.get("success"))
print("Written:", out_file)
'@

$ge_run_ps1 = @'
param()
# Runs Great Expectations check inside Airflow webserver container.
docker compose exec airflow-webserver bash -lc `
  "python /opt/airflow/great_expectations/run_checkpoint.py"
'@

Write-Text (Join-Path $GeRoot 'requirements.txt') $ge_requirements
Write-Text (Join-Path $GeRoot 'run_checkpoint.py') $ge_runner
Write-Text (Join-Path $GeRoot 'run_ge.ps1') $ge_run_ps1

Write-Host "Scaffold created:"
Write-Host " - $DbtRoot"
Write-Host " - $GeRoot"
Write-Host "Next:"
Write-Host " - Use 'dbt\run_dbt.ps1' after containers are up to build models."
Write-Host " - Ensure the Airflow image has Great Expectations installed if you plan to run 'great_expectations\run_ge.ps1'."

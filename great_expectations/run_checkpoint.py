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

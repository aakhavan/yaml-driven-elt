# YAML‑Driven ELT (Airflow + Postgres + MinIO + dbt + Great Expectations)

This repository is a POC that demonstrates driving ELT end‑to‑end from a single YAML config. It ingests CSVs from MinIO (S3‑compatible) to Postgres, optionally runs Great Expectations validations per table in isolated containers, and is ready to add dbt transforms.

Key highlights
- One YAML config per source defines: source (MinIO), destination (schema/table/mode), optional table_definition, and quality checks.
- Airflow runs a single dynamic DAG: one mapped ingest task per table, then one mapped Great Expectations task per table.
- Great Expectations runs in its own container (no dependency conflicts with Airflow) and can upload validation JSON to MinIO.
- MinIO is auto‑seeded at startup with CSVs via a dedicated seeder image that waits until MinIO is ready.
- Concurrency‑safe schema/table creation (advisory locks) and human‑friendly mapped task names (schema.table / GE schema.table).

Repository layout
- `airflow/dags/olist_ingest.py` – main DAG that reads `configs/olist.yml`, runs ingest, then per‑table GX in DockerOperator.
- `airflow/plugins/operators/yaml_ingest_operator.py` – YAML‑driven ingest operator (S3 CSV -> Postgres), with quality checks and advisory locks.
- `airflow/plugins/operators/docker_with_display.py` – small wrapper to show friendly names for mapped DockerOperator tasks.
- `airflow/dags/configs/olist.yml` – the configuration that drives ingest and (optionally) quality rules.
- `docker/Dockerfile` – Airflow image (with providers installed via uv/pip).  
  `docker/Dockerfile.gx` – Great Expectations runner image.  
  `docker/Dockerfile.minio`, `docker/minio-seed.sh` – MinIO seeding image + script (creates bucket, uploads CSVs).
- `docker-compose.yml` – brings up Postgres (meta + warehouse), MinIO, Airflow (init/web/scheduler), and the gx image for ad‑hoc runs.
- `great_expectations/gx_runner.py` – runner script used by the GX container; reads YAML config via env and uploads validation JSON to MinIO if configured.
- `requirements/*.txt` – dependency lists for Airflow providers and project code.
- `data/` – CSVs (Olist datasets) that will be uploaded to MinIO (bucket `olist`) by the seeder.

Prerequisites
- Windows with Docker Desktop and Docker Compose v2
- Python not required on host for running the stack (Airflow/GX/dbt run in containers)

Quickstart (Windows cmd.exe)
1) Configure `.env` (already provided). Key variables:
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`: MinIO admin credentials
- `DEFAULT_DATA_BUCKET`: default bucket (e.g., `olist`) used for GX uploads
- `AIRFLOW__WEBSERVER__SECRET_KEY`, `AIRFLOW__CORE__FERNET_KEY`: Airflow secrets
- `AIRFLOW_CONN_MINIO_S3`, `AIRFLOW_CONN_POSTGRES_WAREHOUSE`: Airflow connections (used by operators)

2) Put CSVs in `data/` (already in this repo for Olist). They will be uploaded to `olist` bucket.

3) Build images (Airflow, GX, MinIO seeder):
```cmd
docker compose build airflow-webserver airflow-scheduler gx-runner minio-seed
```

4) Start infra and seed MinIO:
```cmd
docker compose up -d postgres-meta postgres-warehouse minio
docker compose up --no-recreate minio-seed
docker logs -f minio-seed
```
Expected: alias configured, bucket created, CSVs uploaded.

5) Initialize Airflow and start services:
```cmd
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (default admin created: username `airflow`, password `airflow`).
MinIO Console: http://localhost:9001 (use `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`).

6) Run the pipeline:
- Open Airflow UI and trigger DAG `olist_ingest`.
- For each table in `configs/olist.yml`, you’ll see:
  - Ingest task named like `olist.stg_orders` (reads CSV from MinIO -> copies to Postgres, with optional SQL checks)
  - GE task named like `GE olist.stg_orders` (runs Great Expectations in a container, warn‑only)
- GX validation JSONs are uploaded to MinIO if `DEFAULT_DATA_BUCKET` (or `GX_UPLOAD_BUCKET`) is set.

How the YAML drives everything
- `sync.source`: S3 location in MinIO (bucket + key per table)
- `sync.destination`: default schema; optional table prefix
- `tables[].table`: schema/table name and write mode (append or truncate‑insert)
- `tables[].table_definition`: optional column definitions (types, nullable, PKs)
- `tables[].quality`: optional checks, e.g.:
```yaml
table:
  checks:
    - name: row_count_min
      min: 1
columns:
  - name: review_id
    checks: [{ name: not_null }, { name: unique }]
```
The ingest operator enforces simple SQL checks; the GX step translates the same quality settings to GE expectations where possible.

Great Expectations container behavior
- The DAG uses DockerOperator to run `yaml_driven_elt/gx-runner:latest` per table.
- The container gets config inline via `CONFIG_YAML` env (no volume mount needed).
- It connects to `postgres-warehouse` over the compose network and runs only the selected table (`--only <table_id>`).
- Warn‑only: the command ends with `|| true` so validations do not fail the DAG in this POC. Remove `|| true` to enforce failures.
- Uploads to MinIO: the runner uploads each validation JSON to `s3://<GX_UPLOAD_BUCKET or DEFAULT_DATA_BUCKET>/<GX_S3_PREFIX>/...`. Defaults:
  - `GX_S3_PREFIX=ge/validations`, `S3_ENDPOINT_URL=http://minio:9000`, `S3_VERIFY_SSL=false`

MinIO seeding (robust)
- `minio-seed` builds from `quay.io/minio/mc` and runs `docker/minio-seed.sh`.
- It retries `mc alias set` and `mc ls` until MinIO is ready, creates bucket (`olist`), and uploads CSVs from `data/`.

Development tips
- DAGs and plugins are volume‑mounted; restart Airflow containers to reload code: `docker compose restart airflow-webserver airflow-scheduler`.
- If you change dependencies or Dockerfiles, rebuild affected images: `docker compose build …`.
- The DockerOperator requires `/var/run/docker.sock` mounted in webserver/scheduler (configured in `docker-compose.yml`).

Troubleshooting
- GX image not found / pulls from registry: build locally and ensure `force_pull=False` in the DAG.
- DockerOperator cannot connect: ensure Docker Desktop is running and `/var/run/docker.sock` is mounted.
- MinIO seeding errors: the seeder will retry; check `docker logs -f minio-seed` and confirm `.env` creds.
- Network name: DAG uses `yaml_driven_elt_default`. If your compose project name differs, set `COMPOSE_PROJECT_NAME=yaml_driven_elt` before `docker compose up`, or edit the DAG’s `network_mode`.

Next steps
- Add dbt containerized steps (e.g., run deps/run/test) after ingest and before GX.
- Persist GE artifacts to a shared volume (in addition to MinIO) if you want local copies under `great_expectations/validations/`.
- Expand the YAML schema and generation to more sources and targets.

License
- Licensed under the Apache License, Version 2.0. See the `LICENSE` file for details.

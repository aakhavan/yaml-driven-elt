from __future__ import annotations
from datetime import datetime
import os
import yaml
from airflow import DAG
from airflow.decorators import task
from operators.yaml_ingest_operator import YamlDrivenIngestOperator
from operators.docker_with_display import DockerOperatorWithDisplay


with DAG(
    dag_id="olist_ingest",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
    tags=["poc", "olist", "s3"],
) as dag:

    @task
    def build_pipeline_configs() -> list[dict]:
        cfg_path = os.path.join(os.path.dirname(__file__), "configs", "olist.yml")
        with open(cfg_path, "r", encoding="utf-8") as f:
            root = yaml.safe_load(f)

        sync = root["sync"]
        src = sync["source"]
        dst = sync["destination"]

        default_schema = dst.get("default_schema", "olist")
        table_prefix = dst.get("table_prefix", "")

        configs: list[dict] = []
        for t in root.get("tables", []):
            table_block = t.get("table", {}) or {}
            table_name = table_block.get("name") or f"{table_prefix}{t['id']}"
            schema = table_block.get("schema", default_schema)

            write_mode = table_block.get("write_mode")
            if not write_mode:
                if_exists = table_block.get("if_exists", "append").lower()
                write_mode = "truncate-insert" if if_exists in ("replace", "overwrite") else "append"

            config = {
                "pipeline_id": f"{sync.get('name','sync')}.{t['id']}",
                "source": {
                    "type": "s3_csv",
                    "conf": {"bucket": src["bucket"], "key": t["source"]["key"]},
                },
                "destination": {
                    "schema": schema,
                    "table": table_name,
                    "write_mode": write_mode,
                },
                "table_definition": t.get("table_definition", {}),
                "quality": t.get("quality", {}),
                # include conn ids per-config; operator reads them from cfg
                "source_conn_id": src.get("conn_id", "minio_s3"),
                "dest_conn_id": dst.get("conn_id", "postgres_warehouse"),
                # for display in mapped task name
                "display_name": f"{schema}.{table_name}",
                # keep the id for GX mapping
                "table_id": t.get("id"),
            }
            configs.append(config)

        return configs

    @task
    def load_config_text() -> str:
        cfg_path = os.path.join(os.path.dirname(__file__), "configs", "olist.yml")
        with open(cfg_path, "r", encoding="utf-8") as f:
            return f.read()

    @task
    def build_gx_specs(configs: list[dict], config_text: str) -> list[dict]:
        # Build per-table Docker kwargs for GX validations (one dict per table)
        specs: list[dict] = []
        # Pull defaults from Airflow env (injected via docker-compose)
        default_bucket = os.environ.get("DEFAULT_DATA_BUCKET", "olist")
        minio_user = os.environ.get("MINIO_ROOT_USER", "minio")
        minio_pass = os.environ.get("MINIO_ROOT_PASSWORD", "minio123")
        for cfg in configs:
            table_id = cfg.get("table_id")
            display_name = cfg.get("display_name") or cfg.get("destination", {}).get("table")
            cmd = f"python /app/gx_runner.py --only {table_id} || true"
            spec = {
                "command": ["/bin/sh", "-lc", cmd],
                "environment": {
                    "DATABASE_URL": "postgresql+psycopg2://warehouse:warehouse@postgres-warehouse:5432/warehouse",
                    "GX_CONTEXT_ROOT_DIR": "/app/gx",
                    # Inline the YAML for portability
                    "CONFIG_YAML": config_text,
                    # Upload config for MinIO/S3 (fail-safe in gx_runner)
                    "GX_UPLOAD_BUCKET": default_bucket,
                    "GX_S3_PREFIX": "ge/validations",
                    "S3_ENDPOINT_URL": "http://minio:9000",
                    "AWS_ACCESS_KEY_ID": minio_user,
                    "AWS_SECRET_ACCESS_KEY": minio_pass,
                    "AWS_DEFAULT_REGION": "us-east-1",
                    "S3_VERIFY_SSL": "false",
                },
                "display_name": f"GE {display_name}",
            }
            specs.append(spec)
        return specs

    configs = build_pipeline_configs()
    cfg_text = load_config_text()
    gx_specs = build_gx_specs(configs, cfg_text)

    ingest = (
        YamlDrivenIngestOperator
        .partial(
            task_id="ingest_one",
            run_id="{{ run_id }}",
            create_table_if_not_exists=True,
        )
        .expand(config=configs)  # map over the returned list
    )

    # Mapped GX validation containers per table, chained after ingestion
    gx_validate = (
        DockerOperatorWithDisplay
        .partial(
            task_id="gx_validate",
            image="yaml_driven_elt/gx-runner:latest",
            api_version="auto",
            docker_url="unix://var/run/docker.sock",
            auto_remove=True,
            mount_tmp_dir=False,
            # Compose default network name for this project
            network_mode="yaml_driven_elt_default",
            # Do not pull from registry; use local image
            force_pull=False,
        )
        .expand_kwargs(gx_specs)
    )

    configs >> ingest >> gx_validate

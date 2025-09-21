from __future__ import annotations
from datetime import datetime
import os
import yaml
from airflow import DAG
from airflow.decorators import task
from operators.yaml_ingest_operator import YamlDrivenIngestOperator


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
                # include conn ids per\-config; operator reads them from cfg
                "source_conn_id": src.get("conn_id", "minio_s3"),
                "dest_conn_id": dst.get("conn_id", "postgres_warehouse"),
            }
            configs.append(config)

        return configs

    configs = build_pipeline_configs()

    ingest = (
        YamlDrivenIngestOperator
        .partial(
            task_id="ingest_one",
            run_id="{{ run_id }}",
            create_table_if_not_exists=True,
        )
        .expand(config=configs)  # map over the returned list
    )

    configs >> ingest

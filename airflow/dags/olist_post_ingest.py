
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List

from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Match the repo setup
DBT_IMAGE = "ghcr.io/dbt-labs/dbt-postgres:1.7.9"
DOCKER_URL = "tcp://docker-proxy:2375"  # same host:port as in your compose
NETWORK_MODE = "airflow_default"
DBT_PROJECT_HOST_PATH = "/opt/airflow/dbt"
DBT_PROJECT_IN_CONTAINER = "/dbt"


def _pg_sqlalchemy_url(conn_id: str) -> str:
    c = BaseHook.get_connection(conn_id)
    user = c.login or ""
    pwd = c.password or ""
    host = c.host or "localhost"
    port = c.port or 5432
    db = c.schema or ""
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def gx_basic_rowcount_check(
    schema: str = "analytics",
    conn_id: str = "postgres_warehouse",
    fail_if_empty: bool = True,
) -> None:
    import great_expectations as gx

    hook = PostgresHook(postgres_conn_id=conn_id)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "select table_name from information_schema.tables "
            "where table_schema = %s and table_type = 'BASE TABLE'",
            (schema,),
        )
        tables: List[str] = [r[0] for r in cur.fetchall()]

    if not tables:
        msg = f"No tables found in schema '{schema}'"
        if fail_if_empty:
            raise ValueError(msg)
        logging.info("%s; skipping GX validation.", msg)
        return

    context = gx.get_context(context_root_dir="/opt/airflow/great_expectations")
    ds_name = "pg_wh"
    conn_str = _pg_sqlalchemy_url(conn_id)
    existing = {ds.name for ds in context.list_datasources()}
    if ds_name in existing:
        datasource = context.get_datasource(ds_name)
    else:
        datasource = context.sources.add_sql(name=ds_name, connection_string=conn_str)

    failures: List[str] = []
    for tbl in tables:
        asset_name = f"{schema}__{tbl}"
        try:
            asset = datasource.get_asset(asset_name)
        except Exception:
            asset = datasource.add_table_asset(name=asset_name, table_name=tbl, schema_name=schema)

        batch_request = asset.build_batch_request()
        validator = context.get_validator(batch_request=batch_request)
        validator.expect_table_row_count_to_be_between(min_value=1)
        result = validator.validate(raise_exceptions=False)
        if not result.success:
            failures.append(tbl)

    if failures:
        raise AssertionError(f"Row-count expectation failed for: {', '.join(failures)}")


def dbt_env_from_conn(conn_id: str, schema: str = "analytics") -> Dict[str, str]:
    c = BaseHook.get_connection(conn_id)
    return {
        "DBT_HOST": c.host or "",
        "DBT_USER": c.login or "",
        "DBT_PASSWORD": c.password or "",
        "DBT_DBNAME": c.schema or "",
        "DBT_SCHEMA": schema,
        "DBT_PORT": str(c.port or 5432),
    }


with DAG(
    dag_id="olist_post_ingest",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "dbt", "postgres", "great-expectations"],
) as dag:
    dbt_env = dbt_env_from_conn("postgres_warehouse", schema="analytics")

    mounts = [
        Mount(
            source=DBT_PROJECT_HOST_PATH,
            target=DBT_PROJECT_IN_CONTAINER,
            type="bind",
            read_only=False,
        )
    ]

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image=DBT_IMAGE,
        docker_url=DOCKER_URL,           # connect via TCP, not the unix socket
        api_version="auto",
        auto_remove=True,
        command=[
            "dbt", "deps",
            "--profiles-dir", f"{DBT_PROJECT_IN_CONTAINER}/profiles",
            "--profile", "olist_pg",
        ],
        environment=dbt_env,
        network_mode=NETWORK_MODE,
        mount_tmp_dir=False,
        mounts=mounts,
        working_dir=DBT_PROJECT_IN_CONTAINER,
    )

    dbt_build = DockerOperator(
        task_id="dbt_build",
        image=DBT_IMAGE,
        docker_url=DOCKER_URL,           # connect via TCP, not the unix socket
        api_version="auto",
        auto_remove=True,
        command=[
            "dbt", "build",
            "--profiles-dir", f"{DBT_PROJECT_IN_CONTAINER}/profiles",
            "--profile", "olist_pg",
        ],
        environment=dbt_env,
        network_mode=NETWORK_MODE,
        mount_tmp_dir=False,
        mounts=mounts,
        working_dir=DBT_PROJECT_IN_CONTAINER,
    )

    ge_check_analytics = PythonOperator(
        task_id="gx_rowcount_checks_analytics",
        python_callable=gx_basic_rowcount_check,
        op_kwargs={"schema": "analytics", "conn_id": "postgres_warehouse", "fail_if_empty": True},
    )

    dbt_deps >> dbt_build >> ge_check_analytics

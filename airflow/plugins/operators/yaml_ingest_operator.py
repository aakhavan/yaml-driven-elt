from __future__ import annotations

import io
import csv
import logging
from typing import Any, Dict, List, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import yaml
import uuid

def _read_s3_csv_to_bytes(conf: Dict[str, Any], aws_conn_id: str) -> io.BytesIO:
    import boto3
    conn = BaseHook.get_connection(aws_conn_id)
    extra = conn.extra_dejson or {}
    session = boto3.session.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=extra.get("region_name", "us-east-1"),
    )
    client = session.client("s3", endpoint_url=extra.get("endpoint_url"))
    obj = client.get_object(Bucket=conf["bucket"], Key=conf["key"])
    data = obj["Body"].read()
    buf = io.BytesIO(data)
    buf.seek(0)
    return buf


SOURCE_HANDLERS = {"s3_csv": _read_s3_csv_to_bytes}


def _sanitize_identifier(name: str) -> str:
    out = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name.strip().lower())
    if not out:
        out = "col"
    if out[0].isdigit():
        out = f"c_{out}"
    return out

def _quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'

def _pg_type(spec: Dict[str, Any] | str) -> str:
    if isinstance(spec, str):
        t = spec.lower()
        params = {}
    else:
        t = str(spec.get("type", "string")).lower()
        params = spec
    match t:
        case "string" | "text":
            return "TEXT"
        case "integer" | "int":
            return "INTEGER"
        case "bigint":
            return "BIGINT"
        case "float" | "double":
            return "DOUBLE PRECISION"
        case "decimal" | "numeric":
            p = int(params.get("precision", 18))
            s = int(params.get("scale", 6))
            return f"NUMERIC({p},{s})"
        case "datetime" | "timestamp":
            return "TIMESTAMP"
        case "date":
            return "DATE"
        case "boolean" | "bool":
            return "BOOLEAN"
        case _:
            return "TEXT"


def _normalize_quality(quality: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Accepts either:
      legacy:
        table: { row_count_min: 1 }
        columns: [ { name, not_null, unique, allowed_values, regex, min_length, max_length } ]
      checks-list:
        table: { checks: [ { name: row_count_min, min: 1 } ] }
        columns: [ { name, checks: [ { name: not_null }, { name: unique }, ... ] } ]
    Returns the legacy dict used by _run_quality_checks.
    """
    if not quality:
        return {}
    out: Dict[str, Any] = {"table": {}, "columns": []}
    table_q = quality.get("table") or {}
    if "row_count_min" in table_q:
        out["table"]["row_count_min"] = int(table_q["row_count_min"])
    elif "checks" in table_q:
        for c in table_q["checks"]:
            if c.get("name") == "row_count_min":
                out["table"]["row_count_min"] = int(c.get("min", 1))

    for col in quality.get("columns", []) or []:
        name = col.get("name")
        dest: Dict[str, Any] = {"name": name}
        if "checks" in col:
            for chk in col["checks"]:
                n = chk.get("name")
                if n in ("not_null", "unique"):
                    dest[n] = True
                elif n == "allowed_values":
                    dest["allowed_values"] = chk.get("values", [])
                elif n == "regex":
                    dest["regex"] = chk.get("pattern", "")
                elif n == "min_length":
                    dest["min_length"] = int(chk.get("min", 0))
                elif n == "max_length":
                    dest["max_length"] = int(chk.get("max", 0))
        else:
            # passthrough legacy keys
            for k in ("not_null", "unique", "allowed_values", "regex", "min_length", "max_length"):
                if k in col:
                    dest[k] = col[k]
        out["columns"].append(dest)
    return out


def _run_quality_checks(hook: PostgresHook, schema: str, table: str, quality: Dict[str, Any]) -> Dict[str, Any]:
    summary = {"passed": True, "checks": []}
    if not quality:
        return summary
    full_table = f'"{schema}"."{table}"'
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            table_rules = quality.get("table", {}) or {}
            if "row_count_min" in table_rules:
                cur.execute(f"SELECT COUNT(*) FROM {full_table}")
                rc = cur.fetchone()[0]
                ok = rc >= int(table_rules["row_count_min"])
                summary["checks"].append({"type": "row_count_min", "min": int(table_rules["row_count_min"]), "actual": rc, "passed": ok})
                if not ok:
                    summary["passed"] = False

            for rule in quality.get("columns", []) or []:
                col = rule["name"]
                col_id = f'"{_sanitize_identifier(col)}"'

                if rule.get("not_null"):
                    cur.execute(f"SELECT COUNT(*) FROM {full_table} WHERE {col_id} IS NULL")
                    cnt = cur.fetchone()[0]
                    ok = cnt == 0
                    summary["checks"].append({"type": "not_null", "column": col, "violations": cnt, "passed": ok})
                    if not ok:
                        summary["passed"] = False

                if rule.get("unique"):
                    cur.execute(
                        f"SELECT COUNT(*) FROM (SELECT {col_id} FROM {full_table} "
                        f"WHERE {col_id} IS NOT NULL GROUP BY {col_id} HAVING COUNT(*) > 1) d"
                    )
                    dup_groups = cur.fetchone()[0]
                    ok = dup_groups == 0
                    summary["checks"].append({"type": "unique", "column": col, "duplicate_groups": dup_groups, "passed": ok})
                    if not ok:
                        summary["passed"] = False

                if "allowed_values" in rule and rule["allowed_values"]:
                    allowed = rule["allowed_values"]
                    placeholders = ", ".join(["%s"] * len(allowed))
                    cur.execute(
                        f"SELECT COUNT(*) FROM {full_table} WHERE {col_id} IS NOT NULL AND {col_id} NOT IN ({placeholders})",
                        allowed,
                    )
                    cnt = cur.fetchone()[0]
                    ok = cnt == 0
                    summary["checks"].append({"type": "allowed_values", "column": col, "violations": cnt, "passed": ok})
                    if not ok:
                        summary["passed"] = False

                if "regex" in rule and rule["regex"]:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {full_table} WHERE {col_id} IS NOT NULL AND {col_id} !~ %s",
                        (rule["regex"],),
                    )
                    cnt = cur.fetchone()[0]
                    ok = cnt == 0
                    summary["checks"].append({"type": "regex", "column": col, "violations": cnt, "passed": ok})
                    if not ok:
                        summary["passed"] = False

                if "min_length" in rule:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {full_table} WHERE {col_id} IS NOT NULL AND LENGTH({col_id}) < %s",
                        (int(rule["min_length"]),),
                    )
                    cnt = cur.fetchone()[0]
                    ok = cnt == 0
                    summary["checks"].append({"type": "min_length", "column": col, "violations": cnt, "passed": ok})
                    if not ok:
                        summary["passed"] = False

                if "max_length" in rule:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {full_table} WHERE {col_id} IS NOT NULL AND LENGTH({col_id}) > %s",
                        (int(rule["max_length"]),),
                    )
                    cnt = cur.fetchone()[0]
                    ok = cnt == 0
                    summary["checks"].append({"type": "max_length", "column": col, "violations": cnt, "passed": ok})
                    if not ok:
                        summary["passed"] = False

    return summary


class YamlDrivenIngestOperator(BaseOperator):
    """
    YAML-driven ingest: S3 CSV -> Postgres with optional typed table_definition and quality.
    - Uses table_definition if provided, else infers TEXT columns from header.
    - COPY uses an explicit column list aligned to the CSV header to avoid ordering issues.
    """

    template_fields = ("config_path", "run_id")

    def __init__(
        self,
        *,
        config_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        source_conn_id: str = "minio_s3",
        dest_conn_id: str = "postgres_warehouse",
        run_id: Optional[str] = None,
        create_table_if_not_exists: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.config_path = config_path
        self.inline_config = config
        self.source_conn_id = source_conn_id
        self.dest_conn_id = dest_conn_id
        self.run_id = run_id
        self.create_table_if_not_exists = create_table_if_not_exists

    def _read_header(self, buf: io.BytesIO) -> List[str]:
        buf.seek(0)
        with io.TextIOWrapper(io.BytesIO(buf.getvalue()), encoding="utf-8", newline="") as tbuf:
            reader = csv.reader(tbuf)
            headers = next(reader)
        return headers

    def execute(self, context: Context) -> Dict[str, Any]:
        # 1) Load config
        if self.inline_config is not None:
            cfg = self.inline_config
        elif self.config_path:
            with open(self.config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
        else:
            raise ValueError("Either 'config' or 'config_path' must be provided.")

        pipeline_id = cfg.get("pipeline_id", "unknown_pipeline")
        source = cfg["source"]
        destination = cfg["destination"]
        write_mode = destination.get("write_mode", "append")
        schema = destination["schema"]
        table = destination["table"]
        quality = _normalize_quality(cfg.get("quality"))
        table_def = cfg.get("table_definition") or {}

        # 2) Read CSV into memory
        handler = SOURCE_HANDLERS.get(source["type"])
        if not handler:
            raise ValueError(f"Unsupported source.type: {source['type']}")
        csv_buf = handler(source["conf"], cfg.get("source_conn_id", self.source_conn_id))

        # 3) Prepare DDL pieces
        headers = self._read_header(csv_buf)
        header_ids = [_sanitize_identifier(h) for h in headers]

        defined_cols = {}
        for col in (table_def.get("columns") or []):
            cname = _sanitize_identifier(col["name"])
            defined_cols[cname] = {
                "sql_type": _pg_type(col),
                "nullable": bool(col.get("nullable", True)),
                "original": col["name"],
            }

        # Ensure all header columns exist (TEXT if not in definition)
        ordered_cols_for_create: List[str] = []
        col_defs_sql: List[str] = []
        for cname, original in zip(header_ids, headers):
            ordered_cols_for_create.append(cname)
            if cname in defined_cols:
                sql_type = defined_cols[cname]["sql_type"]
                nullable = defined_cols[cname]["nullable"]
            else:
                sql_type = "TEXT"
                nullable = True
            nn = "NOT NULL" if not nullable else ""
            col_defs_sql.append(f'"{cname}" {sql_type} {nn}'.strip())

        # Append any extra defined columns not present in header
        extra_defined = [c for c in defined_cols.keys() if c not in header_ids]
        for cname in extra_defined:
            ordered_cols_for_create.append(cname)
            sql_type = defined_cols[cname]["sql_type"]
            nullable = defined_cols[cname]["nullable"]
            nn = "NOT NULL" if not nullable else ""
            col_defs_sql.append(f'"{cname}" {sql_type} {nn}'.strip())

        pk_cols = [_sanitize_identifier(c) for c in (table_def.get("primary_key") or [])]
        pk_sql = f", PRIMARY KEY ({', '.join(_quote_ident(c) for c in pk_cols)})" if pk_cols else ""

        # 4) Connect Postgres and create schema/table if needed
        hook = PostgresHook(postgres_conn_id=cfg.get("dest_conn_id", self.dest_conn_id))
        conn = hook.get_conn()
        conn.autocommit = True
        full_table = f'"{schema}"."{table}"'
        copy_cols_list = ", ".join(f'"{c}"' for c in header_ids)

        with conn.cursor() as cursor:
            # Ensure schema and table exist
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            if col_defs_sql:
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table} ({', '.join(col_defs_sql)}{pk_sql})")
            else:
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table} (data TEXT)")

            if write_mode == "truncate-insert":
                cursor.execute(f"TRUNCATE TABLE {full_table}")

            # If no PKs, do a straight COPY as before
            if not pk_cols:
                csv_buf.seek(0)
                copy_sql = f"COPY {full_table} ({copy_cols_list}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                cursor.copy_expert(copy_sql, csv_buf)
            else:

                temp_name = f"_stg_{_sanitize_identifier(table)}_{uuid.uuid4().hex[:8]}"
                temp_qname = f'"{temp_name}"'

                # IMPORTANT: no "ON COMMIT DROP" when autocommit is True; we drop explicitly after use
                cursor.execute(f"CREATE TEMP TABLE {temp_qname} (LIKE {full_table} INCLUDING DEFAULTS)")

                try:
                    # COPY into temp
                    csv_buf.seek(0)
                    copy_tmp_sql = f"COPY {temp_qname} ({copy_cols_list}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                    cursor.copy_expert(copy_tmp_sql, csv_buf)

                    cols_list_q = ", ".join(_quote_ident(c) for c in header_ids)
                    pk_list_q = ", ".join(_quote_ident(c) for c in pk_cols)

                    # Deduplicate inside temp by PK using ROW_NUMBER over ctid
                    dedup_select = (
                        f"SELECT {cols_list_q} "
                        f"FROM (SELECT {cols_list_q}, ROW_NUMBER() OVER (PARTITION BY {pk_list_q} ORDER BY ctid DESC) rn FROM {temp_qname}) t "
                        f"WHERE rn = 1"
                    )

                    if write_mode == "append":
                        insert_sql = (
                            f"INSERT INTO {full_table} ({cols_list_q}) "
                            f"{dedup_select} "
                            f"ON CONFLICT ({pk_list_q}) DO NOTHING"
                        )
                    else:
                        # truncate-insert already cleared the table; no need for ON CONFLICT
                        insert_sql = f"INSERT INTO {full_table} ({cols_list_q}) {dedup_select}"

                    cursor.execute(insert_sql)
                finally:
                    # Clean up temp table explicitly
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_qname}")

        # 5) Quality checks
        qc_summary = _run_quality_checks(hook, schema, table, quality)

        result = {
            "pipeline_id": pipeline_id,
            "destination_table": f"{schema}.{table}",
            "write_mode": write_mode,
            "quality_passed": qc_summary["passed"],
            "quality": qc_summary,
            "run_id": self.run_id,
        }
        if not qc_summary["passed"]:
            self.log.error("Quality checks failed: %s", qc_summary)
            raise ValueError("Quality checks failed")
        self.log.info("Ingest complete: %s", result)
        return result
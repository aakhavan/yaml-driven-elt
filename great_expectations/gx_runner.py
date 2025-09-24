import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List

import yaml
from sqlalchemy import create_engine

# Great Expectations legacy Dataset API (works with GE 0.18.x)
try:
    from great_expectations.dataset import SqlAlchemyDataset  # type: ignore
except Exception as e:  # pragma: no cover
    print(f"Failed to import Great Expectations SqlAlchemyDataset: {e}", file=sys.stderr)
    sys.exit(2)


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_engine_from_env() -> Any:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL env var is required, e.g. postgresql+psycopg2://user:pass@host:5432/dbname"
        )
    return create_engine(database_url)


def apply_table_expectations(dataset: SqlAlchemyDataset, checks: List[Dict[str, Any]]):
    for chk in checks or []:
        name = chk.get("name")
        if name == "row_count_min":
            min_rows = int(chk.get("min", 1))
            dataset.expect_table_row_count_to_be_greater_than(value=min_rows - 1)
        elif name == "row_count_between":
            min_v = chk.get("min")
            max_v = chk.get("max")
            dataset.expect_table_row_count_to_be_between(min_value=min_v, max_value=max_v)
        # extend with more table-level expectations as needed


def apply_column_expectations(dataset: SqlAlchemyDataset, col_name: str, checks: List[Dict[str, Any]]):
    for chk in checks or []:
        name = chk.get("name")
        if name == "not_null":
            dataset.expect_column_values_to_not_be_null(col_name)
        elif name == "unique":
            dataset.expect_column_values_to_be_unique(col_name)
        elif name == "allowed_values":
            values = chk.get("values", [])
            dataset.expect_column_values_to_be_in_set(col_name, values)
        elif name == "regex":
            pattern = chk.get("pattern")
            if pattern:
                dataset.expect_column_values_to_match_regex(col_name, pattern)
        elif name == "range":
            min_v = chk.get("min")
            max_v = chk.get("max")
            dataset.expect_column_values_to_be_between(col_name, min_value=min_v, max_value=max_v)
        # add more mappings as needed


def run_for_table(engine, schema: str, table_name: str, tbl_cfg: Dict[str, Any], output_dir: str) -> Dict[str, Any]:
    ds = SqlAlchemyDataset(table_name=table_name, engine=engine, schema=schema)

    # Table-level checks
    table_checks = (tbl_cfg.get("quality") or {}).get("table", {}).get("checks", [])
    apply_table_expectations(ds, table_checks)

    # Column-level checks
    for col_cfg in ((tbl_cfg.get("quality") or {}).get("columns") or []):
        col_name = col_cfg.get("name")
        if not col_name:
            continue
        apply_column_expectations(ds, col_name, col_cfg.get("checks", []))

    results = ds.validate()

    # Persist result JSON
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(output_dir, f"{schema}.{table_name}_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    return results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Great Expectations validations from a YAML config against Postgres.")
    p.add_argument("--config", default="/app/config.yml", help="Path to config YAML (olist.yml)")
    p.add_argument("--source-id", default=None, help="Optional sync.name to filter (e.g., olist_poc)")
    p.add_argument("--only", nargs="*", default=None, help="Optional list of table ids to run (matches 'tables[].id')")
    p.add_argument("--output", default="/app/output", help="Directory to write validation result JSONs")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_config(args.config)

    sync = cfg.get("sync", {})
    if args.source_id and sync.get("name") != args.source_id:
        print(f"Config sync.name {sync.get('name')} does not match --source-id {args.source_id}. Nothing to do.")
        return 0

    dest = sync.get("destination", {})
    schema = dest.get("default_schema", "public")

    tables: List[Dict[str, Any]] = cfg.get("tables", [])
    if args.only:
        wanted = set(args.only)
        tables = [t for t in tables if t.get("id") in wanted]

    if not tables:
        print("No tables selected to validate.")
        return 0

    engine = build_engine_from_env()

    any_failed = False
    summary = []
    for t in tables:
        table_name = t.get("table", {}).get("name")
        if not table_name:
            print(f"Skipping table with missing name: {t.get('id')}")
            continue
        print(f"Running expectations for {schema}.{table_name}...")
        res = run_for_table(engine, schema, table_name, t, args.output)
        success = bool(res.get("success", False))
        summary.append({
            "table": f"{schema}.{table_name}",
            "success": success,
            "statistics": res.get("statistics", {}),
        })
        if not success:
            any_failed = True

    print("Validation Summary:")
    print(json.dumps(summary, indent=2))

    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())


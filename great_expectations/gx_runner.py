import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List

import yaml
from sqlalchemy import create_engine, inspect

# Use modern Great Expectations API (works with GE 0.18+ and 1.x)
try:
    import great_expectations as gx
except Exception as e:
    print(f"Failed to import great_expectations: {e}", file=sys.stderr)
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


def ensure_sql_datasource(context: Any, name: str, connection_string: str):
    sources = context.sources
    if hasattr(sources, "add_or_update_sql"):
        return sources.add_or_update_sql(name=name, connection_string=connection_string)
    if hasattr(sources, "add_sql"):
        try:
            return sources.add_sql(name=name, connection_string=connection_string)
        except Exception:
            pass
    # Fallback: try to fetch existing datasource by name
    try:
        return context.get_datasource(name)
    except Exception as e:
        raise RuntimeError(f"Could not create or fetch SQL datasource '{name}': {e}")


def apply_table_expectations_validator(validator: Any, checks: List[Dict[str, Any]]):
    for chk in checks or []:
        name = chk.get("name")
        if name == "row_count_min":
            min_rows = int(chk.get("min", 1))
            # Use between with only min_value to support broad GE versions
            validator.expect_table_row_count_to_be_between(min_value=min_rows)
        elif name == "row_count_between":
            min_v = chk.get("min")
            max_v = chk.get("max")
            validator.expect_table_row_count_to_be_between(min_value=min_v, max_value=max_v)


def apply_column_expectations_validator(validator: Any, col_name: str, checks: List[Dict[str, Any]]):
    for chk in checks or []:
        name = chk.get("name")
        if name == "not_null":
            validator.expect_column_values_to_not_be_null(col_name)
        elif name == "unique":
            validator.expect_column_values_to_be_unique(col_name)
        elif name == "allowed_values":
            values = chk.get("values", [])
            validator.expect_column_values_to_be_in_set(col_name, values)
        elif name == "regex":
            pattern = chk.get("pattern")
            if pattern:
                validator.expect_column_values_to_match_regex(col_name, pattern)
        elif name == "range":
            min_v = chk.get("min")
            max_v = chk.get("max")
            validator.expect_column_values_to_be_between(col_name, min_value=min_v, max_value=max_v)


def run_for_table(context: Any, engine, datasource, schema: str, table_name: str, tbl_cfg: Dict[str, Any], output_dir: str) -> Dict[str, Any]:
    # Skip if table does not exist
    inspector = inspect(engine)
    if not inspector.has_table(table_name, schema=schema):
        return {
            "success": False,
            "meta": {"reason": "missing_table", "schema": schema, "table": table_name},
            "statistics": {"evaluated_expectations": 0, "successful_expectations": 0, "unsuccessful_expectations": 0},
        }

    asset_name = f"{schema}_{table_name}"
    # Create or update table asset
    asset = datasource.add_table_asset(name=asset_name, table_name=table_name, schema_name=schema)
    batch_request = asset.build_batch_request()

    suite_name = f"{schema}.{table_name}_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        try:
            from great_expectations.core.expectation_suite import ExpectationSuite
        except Exception:
            # Older GE import path
            from great_expectations.core import ExpectationSuite  # type: ignore
        suite = ExpectationSuite(expectation_suite_name=suite_name)

    try:
        validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
    except TypeError:
        # Fallback for older API where only name is accepted
        validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

    # Table-level checks
    table_checks = (tbl_cfg.get("quality") or {}).get("table", {}).get("checks", [])
    apply_table_expectations_validator(validator, table_checks)

    # Column-level checks
    for col_cfg in ((tbl_cfg.get("quality") or {}).get("columns") or []):
        col_name = col_cfg.get("name")
        if not col_name:
            continue
        apply_column_expectations_validator(validator, col_name, col_cfg.get("checks", []))

    result = validator.validate()

    # Convert result to serializable dict if possible
    if hasattr(result, "to_json_dict"):
        result_dict = result.to_json_dict()
    else:
        # Fallback best-effort
        try:
            result_dict = json.loads(result.to_json())  # type: ignore[attr-defined]
        except Exception:
            result_dict = {"success": getattr(result, "success", False)}

    # Persist result JSON
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(output_dir, f"{schema}.{table_name}_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result_dict, f, indent=2, default=str)

    return result_dict


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Great Expectations validations from a YAML config against Postgres.")
    p.add_argument("--config", default="/app/config.yml", help="Path to config YAML (olist.yml)")
    p.add_argument("--source-id", default=None, help="Optional sync.name to filter (e.g., olist_poc)")
    p.add_argument("--only", nargs="*", default=None, help="Optional list of table ids to run (matches 'tables[].id')")
    p.add_argument("--output", default="/app/output", help="Directory to write validation result JSONs")
    p.add_argument("--build-docs", action="store_true", help="Build local Data Docs after validation")
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

    # Build SQL engine (only for existence check) and GE context/datasource
    engine = build_engine_from_env()
    database_url = os.getenv("DATABASE_URL")
    context_root = os.getenv("GX_CONTEXT_ROOT_DIR", "/app/gx")
    context = gx.get_context(context_root_dir=context_root)
    datasource = ensure_sql_datasource(context, name="warehouse", connection_string=database_url)

    any_failed = False
    summary = []
    for t in tables:
        table_name = t.get("table", {}).get("name")
        if not table_name:
            print(f"Skipping table with missing name: {t.get('id')}")
            continue
        print(f"Running expectations for {schema}.{table_name}...")
        try:
            res = run_for_table(context, engine, datasource, schema, table_name, t, args.output)
            # Persist/Update the suite after applying expectations (if validator created it)
            try:
                # get validator again to ensure latest expectations and save
                asset = datasource.get_asset(f"{schema}_{table_name}")
                batch_request = asset.build_batch_request()
                suite_name = f"{schema}.{table_name}_suite"
                try:
                    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
                except TypeError:
                    from great_expectations.core.expectation_suite import ExpectationSuite
                    suite = ExpectationSuite(expectation_suite_name=suite_name)
                    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
                validator.save_expectation_suite(discard_failed_expectations=False)
            except Exception:
                pass

            success = bool(res.get("success", False))
            summary.append({
                "table": f"{schema}.{table_name}",
                "success": success,
                "statistics": res.get("statistics", {}),
                "meta": res.get("meta", {}),
            })
            if not success:
                any_failed = True
        except Exception as e:
            summary.append({
                "table": f"{schema}.{table_name}",
                "success": False,
                "error": str(e),
            })
            any_failed = True

    if args.build_docs:
        try:
            context.build_data_docs()
            print(f"Built Data Docs at context: {context_root}")
        except Exception as e:
            print(f"Failed to build Data Docs: {e}")

    print("Validation Summary:")
    print(json.dumps(summary, indent=2))

    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())

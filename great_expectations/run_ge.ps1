param()
# Runs Great Expectations check inside Airflow webserver container.
docker compose exec airflow-webserver bash -lc `
  "python /opt/airflow/great_expectations/run_checkpoint.py"

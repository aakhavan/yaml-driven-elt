param()

# Runs dbt inside the Airflow webserver container using the mounted /opt/airflow/dbt folder.
docker compose exec airflow-webserver bash -lc `
  "cd /opt/airflow/dbt && dbt --no-use-colors deps && dbt seed --profiles-dir profiles && dbt run --profiles-dir profiles && dbt test --profiles-dir profiles"

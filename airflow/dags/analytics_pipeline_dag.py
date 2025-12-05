from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    "analytics_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="cd /opt/airflow/analytics-platform/ingestion && python generate_data.py",
    )

    load_to_snowflake = BashOperator(
        task_id="load_to_snowflake",
        bash_command="cd /opt/airflow/analytics-platform/ingestion && python load_to_snowflake.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/analytics-platform/dbt_project && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/analytics-platform/dbt_project && dbt test",
    )

    generate_data >> load_to_snowflake >> dbt_run >> dbt_test

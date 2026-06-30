"""Sample Airflow DAG used as a flowx Airflow-connector fixture.

The serialized form lives in ``../dags_serialized/example_etl.json``; this raw source
exists so the loader can recover ``python_callable`` bodies for the agentic converter.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


def extract_data(run_date: str, env: str) -> int:
    """Pull source rows for ``run_date`` and stage them. Returns the row count."""
    import pandas as pd

    df = pd.read_parquet(f"s3://data-bucket/{run_date}/raw.parquet")
    df = df[df["env"] == env]
    df.to_parquet(f"s3://staging-bucket/{run_date}/extracted.parquet")
    return len(df)


def choose_branch(**context) -> str:
    """Return the next task_id based on whether any rows were extracted."""
    row_count = context["ti"].xcom_pull(task_ids="extract")
    return "notify" if row_count and row_count > 0 else "cleanup"


with DAG(
    dag_id="example_etl",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    params={"env": "prod"},
    tags=["example", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")
    wait_for_file = S3KeySensor(task_id="wait_for_file", bucket_key="s3://data-bucket/{{ ds }}/_SUCCESS")
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        op_kwargs={"run_date": "{{ ds }}", "env": "{{ params.env }}"},
        retries=3,
    )
    transform = SparkSubmitOperator(
        task_id="transform",
        application="/jobs/transform.py",
        application_args=["--date", "{{ ds }}"],
    )
    run_databricks_job = DatabricksRunNowOperator(task_id="run_databricks_job", job_id=4321)
    branch = BranchPythonOperator(task_id="branch", python_callable=choose_branch)
    notify = BashOperator(task_id="notify", bash_command="echo 'pipeline complete for {{ ds }}'")
    cleanup = EmptyOperator(task_id="cleanup")

    [start, wait_for_file] >> extract >> transform >> run_databricks_job >> branch >> [notify, cleanup]

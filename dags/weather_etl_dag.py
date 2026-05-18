from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_weather_spark",
    default_args=default_args,
    description="ETL Process for Weather Data: S3 -> Spark -> MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 5, 18),
    catchup=False,
    tags={'spark', 'mongodb', 'weather'}
) as dag:
    task_submit = BashOperator(
        task_id="weather_etl",
        bash_command="""
        spark-submit \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/airflow/scripts/spark_etl_forecast.py
        """
    )
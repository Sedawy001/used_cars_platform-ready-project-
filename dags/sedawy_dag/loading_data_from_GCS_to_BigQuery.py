
import logging
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator




dag = DAG(
    dag_id="Dag_02",
    description="LOADING_DATA_FROM_GCS_TO_BIGQUERY",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)


start_task = EmptyOperator(task_id="start_task", dag=dag)


end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> end_task

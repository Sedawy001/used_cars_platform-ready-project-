
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
load_task = GoogleCloudStorageToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='ready-project-dataset',
        source_objects=['cars-com_dataset/*csv'],
        destination_project_dataset_table='ready-data-de24.Cars_02',
        source_format='CSV',
        autodetect=True,
        field_delimiter=';',
        max_bad_records = 100000000,
        ignore_unknown_values = True,
        write_disposition='Write_append',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag,
    )

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> end_task

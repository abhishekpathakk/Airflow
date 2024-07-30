from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Retry delay in case of failure
}

with DAG(
    'spark_job_dag',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name='my-spark-cluster',
        region='us-east1',
        zone='us-east1-b',
        num_workers=2,
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2',
        project_id='singular-silo-425507-p6',
    )

    submit_job = DataprocSubmitJobOperator(
        task_id='submit_spark_job',
        job={
            'reference': {
                'project_id': 'singular-silo-425507-p6'
            },
            'placement': {
                'cluster_name': 'my-spark-cluster'
            },
            'pyspark_job': {  # Correct field for PySpark jobs
                'main_python_file_uri': 'gs://data-ps/users_data_processing.py',
                'args': ['gs://data-ps/input_file.csv', 'gs://data-ps/output_file.csv'],
            },
        },
        region='us-east1',
        project_id='singular-silo-425507-p6',
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='my-spark-cluster',
        region='us-east1',
        project_id='singular-silo-425507-p6',
        trigger_rule='all_done',  # Ensure this runs even if the previous task fails
    )

    create_cluster >> submit_job >> delete_cluster

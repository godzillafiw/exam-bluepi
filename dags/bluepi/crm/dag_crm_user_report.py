"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'crm_user_report_exam',
    default_args=default_args,
    description='load data from database',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20))

DESTINATION_BUCKET = 'datalake-bluepi-bucket'
DESTINATION_DIRECTORY = "blupi/crm"

# priority_weight has type int in Airflow DB, uses the maximum.
# t1 = BashOperator(
#     task_id='echo',
#     bash_command='echo test',
#     dag=dag,
#     depends_on_past=False,
#     priority_weight=2**31-1)

table_name = 'users'

ingest_data = PostgresToGoogleCloudStorageOperator(
        task_id="ingest_data",
        dag=dag,
        sql=f"SELECT * FROM {table_name}",
        export_format="csv",
        field_delimiter="|",
        filename=f"{DESTINATION_BUCKET}/{DESTINATION_DIRECTORY}/export_{table_name}/{table_name}.csv",
        bucket=DESTINATION_BUCKET,
        retries=3,
        postgres_conn_id="postgres_crm",
        google_cloud_storage_conn_id="bluepi_gcp_connection",
    )

task_default = BigQueryOperator(
    task_id='task_default_connection',
    dag=dag,
    bql='SELECT 1', use_legacy_sql=False
    )

ingest_data > task_default
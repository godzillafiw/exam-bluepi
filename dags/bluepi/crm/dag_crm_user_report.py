"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

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

DESTINATION_BUCKET = 'gs://datalake-bluepi-bucket'
DESTINATION_DIRECTORY = "blupi/crm"

# priority_weight has type int in Airflow DB, uses the maximum.
# t1 = BashOperator(
#     task_id='echo',
#     bash_command='echo test',
#     dag=dag,
#     depends_on_past=False,
#     priority_weight=2**31-1)

ingest_data = PostgresToGoogleCloudStorageOperator(
        task_id="ingest_data",
        dag=dag,
        bucket=DESTINATION_BUCKET,
        filename=DESTINATION_DIRECTORY + "/{{ execution_date }}" + "/{}.csv",
        sql='''SELECT * FROM users;''',
        retries=3,
        postgres_conn_id="postgres_crm"
    )

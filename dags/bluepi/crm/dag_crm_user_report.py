"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import date,timedelta
from time import gmtime, strftime
import pendulum
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from bluepi.crm.src.etl_helper import *

# Define all variable
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['anuchar_4@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'de_anucha_exam',
    default_args=default_args,
    description='load data from database',
    schedule_interval='@hourly',
    dagrun_timeout=timedelta(minutes=20)
)

# Variable for project
PROJECT_ID = 'de-exam-anucha'
DESTINATION_BUCKET = 'datalake-bluepi-bucket'
DESTINATION_DIRECTORY = 'bluepi/crm'
DATABASE = 'postgres'
TABLE_NAME = 'user_log'
DATASET_NAME = 'dw_bluepi'

# current date and time
TODAY =  date.today().strftime('%Y-%m-%d')
HOURS = strftime("%H%M%S", gmtime())


# Load data from source
ingest_data = PostgresToGoogleCloudStorageOperator(
    task_id="ingest_data",
    dag=dag,
    sql=f"SELECT id, user_id, action, CAST(status AS int), \
                to_char(created_at,'YYYY-MM-DD HH24:MI:SS') AS created_at, \
                to_char(updated_at,'YYYY-MM-DD HH24:MI:SS') AS updated_at \
        FROM {TABLE_NAME}",
    export_format="csv",
    field_delimiter="|",
    filename=f"{DESTINATION_DIRECTORY}/{DATABASE}/dt={TODAY}/{TABLE_NAME}-{TODAY}.csv",
    bucket=DESTINATION_BUCKET,
    retries=2,
    postgres_conn_id="postgres_crm",
    google_cloud_storage_conn_id="bluepi_gcp_connection",
)

# Load data to BigQuery
# load_csv = GoogleCloudStorageToBigQueryOperator(
#     task_id='gcs_to_bq_example',
#     bucket=DESTINATION_BUCKET,
#     source_objects=[f"{DESTINATION_DIRECTORY}/{DATABASE}/dt={TODAY}/{TABLE_NAME}-{TODAY}.csv"],
#     destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
#     source_format='CSV',
#     # create_disposition='CREATE_IF_NEEDED',
#     field_delimiter="|",
#     schema_fields=[
#         {"name": "id", "type": "STRING", "mode": "REQUIRED"},
#         {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "action", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "status", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "created_at", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "updated_at", "type": "STRING", "mode": "NULLABLE"}
#     ],
#     skip_leading_rows=1,
#     write_disposition='WRITE_TRUNCATE',
#     google_cloud_storage_conn_id='bluepi_gcp_connection',
#     dag=dag)

load_tranform_data = PythonOperator(
    task_id='load_tranform_data',
    dag=dag,
    python_callable=tranform_data
)

# Validate data
validate_data = BigQueryOperator(
    task_id='validate_data',
    dag=dag,
    bql=f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}` LIMIT 10",
    use_legacy_sql=False
)

# Define task
ingest_data.set_downstream([load_tranform_data])
load_tranform_data.set_downstream([validate_data])
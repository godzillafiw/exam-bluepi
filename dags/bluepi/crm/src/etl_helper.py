from google.cloud import bigquery
from google.cloud import storage
from datetime import date,timedelta
from time import gmtime, strftime
import pandas
import pytz
import pandas as pd
import io

# Declare variable
PROJECT_ID = 'de-exam-anucha'
DESTINATION_BUCKET = 'datalake-bluepi-bucket'
DESTINATION_DIRECTORY = 'bluepi/crm'
DATABASE = 'postgres'
TABLE_NAME = 'user_log'
DATASET_NAME = 'dw_bluepi'

# current date and time
TODAY =  date.today().strftime('%Y-%m-%d')
HOURS = strftime("%H%M%S", gmtime())

def create_table():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    print('='*50)
    table_id = "dw_bluepi.test"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    uri = "gs://cloud-samples-data/bigquery/us-states/us-states.json"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

# Load data from gcp storage then tranform
def tranform_data():

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Construct a Storage client object.
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(DESTINATION_BUCKET)

    # Create DataFrame
    col_names = ["id", "user_id", "action", "success", "created_at", "updated_at"]
    df_gcs = pd.DataFrame(columns=col_names)

    # for file in list(source_bucket.list_blobs()):
    #     # file_path="gs://{}/{}".format(file.bucket.name, file.name)
    #     print(file.name)
    #     dd=1

    # Get data form google cloud storage
    file_path=f"gs://{DESTINATION_BUCKET}/{DESTINATION_DIRECTORY}/{DATABASE}/dt={TODAY}/{TABLE_NAME}-{TODAY}.csv"
    print(file_path)
    df_gcs = pd.read_csv(file_path, skiprows=1, sep='|', header=None, names=col_names)

    # Tranform colmns and value
    convert_dict = {
        'id': str,
        'user_id': str,
        'action': str,
        'success': bool,
        'created_at' : str,
        'updated_at' : str
    }

    df_gcs = df_gcs.astype(convert_dict)
    df_gcs.success.replace(1, True,inplace=True)
    df_gcs.success.replace(0, False,inplace=True)

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id =f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

    dataframe = pandas.DataFrame(
    df_gcs,
    # In the loaded table, the column order reflects the order of the
    # columns in the DataFrame.
    columns=[
        'id',
        'user_id',
        'action',
        'success',
        'created_at' ,
        'updated_at'
    ],
    # Optionally, set a named index, which can also be written to the
    # BigQuery table.
    # index=pandas.Index(
    #     [u"Q24980", u"Q25043", u"Q24953", u"Q16403"], name="wikidata_id"
    # ),
    )
    job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("action", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("success", bigquery.enums.SqlTypeNames.BOOL),
        bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("updated_at", bigquery.enums.SqlTypeNames.STRING),
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def query_data():

    # GCS Source
    storage_client = storage.Client()
    source_bucket = storage_client.bucket('datalake-bluepi-bucket')

    col_names = ["id", "user_id", "action", "success", "created_at", "updated_at"]
    df = pd.DataFrame(columns=col_names)

    # for file in list(source_bucket.list_blobs()):
    #     # file_path="gs://{}/{}".format(file.bucket.name, file.name)
    #     #print(file.name)
    #     dd=1

    file_path='gs://datalake-bluepi-bucket/bluepi/crm/postgres/dt=2021-02-16/user_log-020010.csv'
    df = pd.read_csv(file_path, skiprows=1, sep='|', header=None, names=col_names)

    convert_dict = {
        'id': str,
        'user_id': str,
        'action': str,
        'success': bool,
        'created_at' : str,
        'updated_at' : str
    }
    df = df.astype(convert_dict)
    df.success.replace(1, True,inplace=True)
    df.success.replace(0, False,inplace=True)
    print(df)
    print(df.info())

# query_data()
tranform_data()
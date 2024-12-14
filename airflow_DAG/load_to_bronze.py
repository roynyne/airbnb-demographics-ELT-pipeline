from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import requests
import logging

# Default arguments for the DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
}

# List of Airbnb files to be processed sequentially
airbnb_files = [
    '05_2020.csv', '06_2020.csv', '07_2020.csv', '08_2020.csv', 
    '09_2020.csv', '10_2020.csv', '11_2020.csv', '12_2020.csv',
    '01_2021.csv', '02_2021.csv', '03_2021.csv', '04_2021.csv'
]

# Define the DAG
with DAG(
    dag_id='load_to_bronze_schema_sequential',
    schedule_interval=None,  # No schedule interval, manually triggered
    default_args=default_args,
    description='Load raw data from GCS to Bronze schema in Postgres sequentially and trigger dbt job',
    catchup=False,
) as dag:

    # Task: Download and load static files (Census and LGA data)
    def load_to_postgres(file_path, table_name):
        postgres_hook = PostgresHook(postgres_conn_id='postgres')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Load CSV data into a Pandas DataFrame
        df = pd.read_csv(file_path)
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # Drop unnamed columns if any

        # Insert DataFrame rows into Postgres table
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(df.columns)
            sql = f"INSERT INTO bronze.{table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()

    # Task: Load Census G01 data
    download_and_load_census_g01 = PythonOperator(
        task_id='download_and_load_census_g01',
        python_callable=lambda: GCSToLocalFilesystemOperator(
            task_id='download_census_g01',
            bucket='australia-southeast1-bde-0c9d64c9-bucket',
            object_name='data/airbnb_part_1/2016Census_G01_NSW_LGA.csv',
            filename='/tmp/2016Census_G01_NSW_LGA.csv'
        ).execute(context=None) or load_to_postgres('/tmp/2016Census_G01_NSW_LGA.csv', 'raw_census_g01')
    )

    # Task: Load Census G02 data
    download_and_load_census_g02 = PythonOperator(
        task_id='download_and_load_census_g02',
        python_callable=lambda: GCSToLocalFilesystemOperator(
            task_id='download_census_g02',
            bucket='australia-southeast1-bde-0c9d64c9-bucket',
            object_name='data/airbnb_part_1/2016Census_G02_NSW_LGA.csv',
            filename='/tmp/2016Census_G02_NSW_LGA.csv'
        ).execute(context=None) or load_to_postgres('/tmp/2016Census_G02_NSW_LGA.csv', 'raw_census_g02')
    )

    # Task: Load LGA Codes data
    download_and_load_lga_codes = PythonOperator(
        task_id='download_and_load_lga_codes',
        python_callable=lambda: GCSToLocalFilesystemOperator(
            task_id='download_lga_codes',
            bucket='australia-southeast1-bde-0c9d64c9-bucket',
            object_name='data/airbnb_part_1/NSW_LGA_CODE.csv',
            filename='/tmp/NSW_LGA_CODE.csv'
        ).execute(context=None) or load_to_postgres('/tmp/NSW_LGA_CODE.csv', 'raw_lga_codes')
    )

    # Task: Load LGA Suburbs data
    download_and_load_lga_suburbs = PythonOperator(
        task_id='download_and_load_lga_suburbs',
        python_callable=lambda: GCSToLocalFilesystemOperator(
            task_id='download_lga_suburbs',
            bucket='australia-southeast1-bde-0c9d64c9-bucket',
            object_name='data/airbnb_part_1/NSW_LGA_SUBURB.csv',
            filename='/tmp/NSW_LGA_SUBURB.csv'
        ).execute(context=None) or load_to_postgres('/tmp/NSW_LGA_SUBURB.csv', 'raw_lga_suburbs')
    )

    # Task: Download and load each Airbnb file sequentially
    def download_and_load_airbnb(file_name):
        GCSToLocalFilesystemOperator(
            task_id=f'download_{file_name}',
            bucket='australia-southeast1-bde-0c9d64c9-bucket',
            object_name=f'data/airbnb_part_1/{file_name}',
            filename=f'/tmp/{file_name}'
        ).execute(context=None)

        load_to_postgres(f'/tmp/{file_name}', 'raw_airbnb_listings')

    # Sequential tasks for each Airbnb file
    previous_task = None
    for file_name in airbnb_files:
        task = PythonOperator(
            task_id=f'download_and_load_{file_name.split(".")[0]}',
            python_callable=download_and_load_airbnb,
            op_kwargs={'file_name': file_name}
        )
        if previous_task:
            previous_task >> task
        previous_task = task

    # Task: Trigger dbt Cloud job after all files are processed
    def trigger_dbt_cloud_job():
        dbt_cloud_url = Variable.get("DBT_CLOUD_URL")
        dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
        dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")
        dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")

        url = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}/jobs/{dbt_cloud_job_id}/run/"
        headers = {
            'Authorization': f'Token {dbt_cloud_token}',
            'Content-Type': 'application/json'
        }
        data = {"cause": "Triggered via API"}

        response = requests.post(url, headers=headers, json=data)
        if response.status_code != 200:
            logging.error(f"Failed to trigger dbt Cloud job: {response.status_code}, {response.text}")
            raise Exception("Failed to trigger dbt Cloud job.")
        logging.info("Successfully triggered dbt Cloud job.")

    trigger_dbt_job = PythonOperator(
        task_id='trigger_dbt_job',
        python_callable=trigger_dbt_cloud_job
    )

    # Set dependencies
    download_and_load_census_g01 >> download_and_load_census_g02 >> download_and_load_lga_codes >> download_and_load_lga_suburbs >> previous_task >> trigger_dbt_job

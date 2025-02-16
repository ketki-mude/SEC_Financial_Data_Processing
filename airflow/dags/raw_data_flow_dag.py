from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from load_airflow_variables import load_airflow_variables
from web_scrapper import download_quarterly_data
from zip_ext_and_parq_store import SECDataProcessor
from snowflake_raw_data_loader import SnowflakeLoader

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'raw_data_pipeline',
    default_args=default_args,
    description='SEC Data Ingestion Pipeline',
    schedule_interval=None,
    catchup=False
)

task_load_variables = PythonOperator(
    task_id='load_airflow_variables',
    python_callable=load_airflow_variables,
    provide_context=True,
    dag=dag
)

# **Step 2: Scrape SEC Data**
def scrape_sec_data(**context):
    """Fetches SEC ZIP files and stores them in S3"""
    year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    download_quarterly_data(year, quarter)

task_scrape_sec = PythonOperator(
    task_id='scrape_sec_data',
    python_callable=scrape_sec_data,
    provide_context=True,
    dag=dag
)

# **Step 3: Extract & Convert Data**
def extract_and_convert(**context):
    """Extracts SEC ZIP files and converts to Parquet"""
    year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    processor = SECDataProcessor()
    processor.extract_zip_file(year, quarter)

task_extract_convert = PythonOperator(
    task_id='extract_and_convert',
    python_callable=extract_and_convert,
    provide_context=True,
    dag=dag
)

# **Step 4: Load Data into Snowflake**
def load_to_snowflake(**context):
    """Loads Parquet data from S3 into Snowflake"""
    year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    loader = SnowflakeLoader(year, quarter)
    loader.create_schema()
    loader.create_tables()
    loader.setup_s3_integration()
    loader.load_data()
    loader.cleanup()

task_load_snowflake = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    provide_context=True,
    dag=dag
)

# **Set Task Dependencies**
task_load_variables >> task_scrape_sec >> task_extract_convert >> task_load_snowflake
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import os
import snowflake.connector
from dotenv import load_dotenv

# Import your processing functions
from sec_data_scrapper import download_quarterly_data
from ext_zip_convert_into_json_store import extract_and_convert_to_json
from load_json_data_snowflake import load_json_data_to_snowflake, get_latest_s3_folder, create_views

# Load environment variables
load_dotenv()

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'json_data_pipeline_v1',
    default_args=default_args,
    description='SEC JSON Data Ingestion Pipeline',
    schedule_interval=None,
    catchup=False
)

# Step 1: Scrape SEC Data
def scrape_sec_data(**kwargs):
    """
    Fetches SEC ZIP files from the SEC website and stores them in S3.
    Reads the year and quarter from Airflow Variables.
    """
    year = int(Variable.get("sec_year", default_var=2023))
    quarter = int(Variable.get("sec_quarter", default_var=1))
    download_quarterly_data(year, quarter)

task_scrape_sec = PythonOperator(
    task_id='scrape_sec_data',
    python_callable=scrape_sec_data,
    dag=dag
)

# Step 2: Extract & Convert Data to JSON
def extract_and_convert_json(**kwargs):
    """
    Extracts SEC ZIP files and converts them to JSON.
    Reads the year and quarter from Airflow Variables.
    """
    year = int(Variable.get("sec_year", default_var=2023))
    quarter = int(Variable.get("sec_quarter", default_var=1))
    extract_and_convert_to_json(year, quarter)

task_extract_convert_json = PythonOperator(
    task_id='extract_and_convert_json',
    python_callable=extract_and_convert_json,
    dag=dag
)

# Step 3: Load JSON Data into Snowflake
def load_json_snowflake(**kwargs):
    """
    Loads JSON data (assumed to be stored in S3) into Snowflake.
    """
    load_json_data_to_snowflake()

task_load_json_snowflake = PythonOperator(
    task_id='load_json_snowflake',
    python_callable=load_json_snowflake,
    dag=dag
)

# Step 4: Create Views in Snowflake
def create_views_in_snowflake_task(**kwargs):
    """
    Opens a new Snowflake connection, retrieves the latest year and quarter,
    builds the dynamic table name, and calls the existing create_views function.
    """
    # Retrieve latest year and quarter using your helper function.
    latest_year, latest_quarter = get_latest_s3_folder()
    SCHEMA_NAME = os.getenv('SCHEMA_NAME', 'SEC_JSON_DATA')
    table_name = f"{SCHEMA_NAME}.sec_data_{latest_year}_{latest_quarter}"
    
    # Open a new Snowflake connection.
    ctx = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE')
    )
    try:
        # Set active schema.
        ctx.cursor().execute(f"USE SCHEMA {SCHEMA_NAME};")
        # Directly call the existing create_views function.
        create_views(ctx, table_name, latest_year, latest_quarter)
    finally:
        ctx.close()

task_create_views = PythonOperator(
    task_id='create_views',
    python_callable=create_views_in_snowflake_task,
    dag=dag
)

# Set Task Dependencies: scrape -> extract/convert -> load -> create views.
task_scrape_sec >> task_extract_convert_json >> task_load_json_snowflake >> task_create_views

dag.doc_md = """
### JSON Data Ingestion Pipeline
This DAG handles:
1. Scraping SEC data (ZIP files) based on the SEC year and quarter stored in Airflow Variables.
2. Extracting and converting the ZIP files to JSON.
3. Loading the JSON data into Snowflake.
4. Creating views in Snowflake based on the loaded table.
"""

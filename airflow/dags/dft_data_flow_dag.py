from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from load_airflow_variables import load_airflow_variables
from web_scrapper import download_quarterly_data
from zip_ext_and_parq_store import SECDataProcessor
from s3_data_checker import is_data_present_in_s3
import subprocess


# **Step 5: Run DBT Pipeline**
DBT_PROJECT_DIR = "/opt/airflow/sec_pipeline"
DBT_PROFILE_DIR = "/opt/airflow/sec_pipeline/profiles"

# **Step 2: Check Data in S3**
def check_data_and_set_variable(**context):
    """Checks if data is present in S3 and sets a variable"""
    year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    
    if is_data_present_in_s3(year, quarter):
        context['task_instance'].xcom_push(key='run_dbt', value=True)
    else:
        context['task_instance'].xcom_push(key='run_dbt', value=False)

def scrape_sec_data(**context):
    """Fetches SEC ZIP files and stores them in S3"""
    year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    download_quarterly_data(year, quarter)

def extract_and_convert(**context):
    """Extracts SEC ZIP files and converts to Parquet"""
    year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    processor = SECDataProcessor()
    processor.extract_zip_file(year, quarter)

def run_dbt_pipeline(**context):
    """Runs the DBT pipeline using subprocess."""
    year = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_year')
    quarter = context['task_instance'].xcom_pull(task_ids='load_airflow_variables', key='sec_quarter')
    
    dbt_command = [
        "dbt", "run",
        "--project-dir", "/opt/airflow/sec_pipeline",
        "--profiles-dir", "/opt/airflow/sec_pipeline/profiles",
        "--vars", f"{{'year': {year}, 'quarter': {quarter}}}"
    ]
    
    subprocess.run(dbt_command, check=True)


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dft_data_pipeline',
    default_args=default_args,
    description='DFT Data Ingestion Pipeline',
    schedule_interval=None,
    catchup=False
)

task_load_variables = PythonOperator(
    task_id='load_airflow_variables',
    python_callable=load_airflow_variables,
    provide_context=True,
    dag=dag
)


task_check_data_and_set_variable = PythonOperator(
    task_id='check_data_and_set_variable',
    python_callable=check_data_and_set_variable,
    provide_context=True,
    dag=dag
)

task_decide_dbt = BranchPythonOperator(
    task_id='decide_dbt',
    python_callable=lambda **context: ['run_dbt_pipeline'] if context['task_instance'].xcom_pull(task_ids='check_data_and_set_variable', key='run_dbt') else ['scrape_sec_data'],
    provide_context=True,
    dag=dag
)

# **Step 3: Scrape SEC Data**
task_scrape_sec = PythonOperator(
    task_id='scrape_sec_data',
    python_callable=scrape_sec_data,
    provide_context=True,
    dag=dag
)

# **Step 4: Extract & Convert Data**
task_extract_convert = PythonOperator(
    task_id='extract_and_convert',
    python_callable=extract_and_convert,
    provide_context=True,
    dag=dag
)


task_run_dbt = BashOperator(
    task_id='run_dbt_pipeline',
    bash_command=(
        "bash /opt/airflow/sec_pipeline/run_dbt_pipeline.sh "
        "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_year') }} "
        "{{ ti.xcom_pull(task_ids='load_airflow_variables', key='sec_quarter') }}"
    ),
    dag=dag
)

# Set Task Dependencies
task_load_variables >> task_check_data_and_set_variable
task_check_data_and_set_variable >> task_decide_dbt
task_decide_dbt >> task_run_dbt
task_decide_dbt >> task_scrape_sec
task_scrape_sec >> task_extract_convert >> task_run_dbt
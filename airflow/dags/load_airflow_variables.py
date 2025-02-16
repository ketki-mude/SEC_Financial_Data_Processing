from airflow.models import Variable


def load_airflow_variables(**context):
    """Loads SEC year & quarter and pushes them to XCom"""
    year = Variable.get('sec_year', default_var=2023)  # Default to 2023 if not set
    quarter = Variable.get('sec_quarter', default_var=1)  # Default to 1 if not set
    context['task_instance'].xcom_push(key='sec_year', value=year)
    context['task_instance'].xcom_push(key='sec_quarter', value=quarter)
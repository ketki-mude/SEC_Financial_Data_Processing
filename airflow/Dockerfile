FROM apache/airflow:2.5.1

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install dbt-core dbt-snowflake ujson
RUN pip install --no-cache-dir -r /requirements.txt
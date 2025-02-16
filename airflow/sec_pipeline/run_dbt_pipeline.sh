#!/bin/bash

set -e  # Exit script immediately on error

# Set the working directory to where dbt_project.yml is located
cd /opt/airflow/sec_pipeline/

# Explicitly set the DBT profiles directory
export DBT_PROFILES_DIR=/opt/airflow/sec_pipeline/profiles

# Accept year and quarter as arguments
YEAR=$1
QUARTER=$2
STAGE_NAME="sec_stage_${YEAR}Q${QUARTER}"
FILE_NAME=${YEAR}Q${QUARTER}
SCHEMA_NAME="SEC_DATA_DFT"

echo -e "\n🚀 Step 0: Installing dependencies..."
dbt deps

echo -e "\n🚀 Step 0.5: Creating schema..."
dbt run-operation create_schema --args '{"schema_name": "'"$SCHEMA_NAME"'"}'

# Add this step before running dbt models
echo -e "\n🚀 Step 1: Creating file format..."
dbt run-operation create_file_format

echo -e "\n🚀 Step 2: Creating Snowflake Stage..."
dbt run-operation create_stage --args '{"stage_name": "'"$STAGE_NAME"'"}'

echo -e "\n🚀 Step 3: Running dbt models to create tables..."
dbt run --select models/staging --vars '{"stage_name": "'"$STAGE_NAME"'", "year": "'"$YEAR"'", "quarter": "'"$QUARTER"'", "file_name": "'"$FILE_NAME"'"}'

echo -e "\n🚀 Step 4: Copying data into tables..."
dbt run-operation copy_into_raw_num --args '{"stage_name": "'"$STAGE_NAME"'", "file_name": "'"$FILE_NAME"'"}'
dbt run-operation copy_into_raw_pre --args '{"stage_name": "'"$STAGE_NAME"'", "file_name": "'"$FILE_NAME"'"}'
dbt run-operation copy_into_raw_sub --args '{"stage_name": "'"$STAGE_NAME"'", "file_name": "'"$FILE_NAME"'"}'
dbt run-operation copy_into_raw_tag --args '{"stage_name": "'"$STAGE_NAME"'", "file_name": "'"$FILE_NAME"'"}'

echo -e "\n🚀 Step 5: Running dbt models to create fact tables..."
dbt run --select models/fact_data_load --vars '{"stage_name": "'"$STAGE_NAME"'", "year": "'"$YEAR"'", "quarter": "'"$QUARTER"'", "file_name": "'"$FILE_NAME"'"}'

echo -e "\n✅ Data load completed successfully!"

echo -e "\n🧪 Step 6: Running dbt tests..."
dbt test --vars '{"stage_name": "'"$STAGE_NAME"'", "year": "'"$YEAR"'", "quarter": "'"$QUARTER"'"}' --store-failures

echo -e "\n✅ All steps completed successfully!"
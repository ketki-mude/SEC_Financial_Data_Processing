import os
import snowflake.connector
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Snowflake connection details
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SCHEMA_NAME = 'sec_data_json'

# SQL commands to create views
CREATE_BALANCE_SHEET_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.view_balance_sheet AS
SELECT 
    s.company,
    value:label::VARCHAR AS label,
    value:concept::VARCHAR AS concept,
    value:info::VARCHAR AS info,
    value:unit::VARCHAR AS unit,
    value:value::FLOAT AS value
FROM {SCHEMA_NAME}.sec_json_data s,
LATERAL FLATTEN(input => s.data:bs) AS value;
"""

CREATE_INCOME_STATEMENT_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.view_income_statement AS
SELECT 
    s.company,
    value:label::VARCHAR AS label,
    value:concept::VARCHAR AS concept,
    value:info::VARCHAR AS info,
    value:unit::VARCHAR AS unit,
    value:value::FLOAT AS value
FROM {SCHEMA_NAME}.sec_json_data s,
LATERAL FLATTEN(input => s.data:is) AS value;
"""

CREATE_CASH_FLOW_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.view_cash_flow AS
SELECT 
    s.company,
    value:label::VARCHAR AS label,
    value:concept::VARCHAR AS concept,
    value:info::VARCHAR AS info,
    value:unit::VARCHAR AS unit,
    value:value::FLOAT AS value
FROM {SCHEMA_NAME}.sec_json_data s,
LATERAL FLATTEN(input => s.data:cf) AS value;
"""

def create_views_in_snowflake():
    try:
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE
        )
        cs = ctx.cursor()
        logger.info("Successfully connected to Snowflake.")

        # Create views
        logger.info("Creating view for balance sheet...")
        cs.execute(CREATE_BALANCE_SHEET_VIEW_SQL)

        logger.info("Creating view for income statement...")
        cs.execute(CREATE_INCOME_STATEMENT_VIEW_SQL)

        logger.info("Creating view for cash flow...")
        cs.execute(CREATE_CASH_FLOW_VIEW_SQL)

        logger.info("Views created successfully.")

    except Exception as e:
        logger.error(f"Error creating views in Snowflake: {e}", exc_info=True)
    finally:
        cs.close()
        ctx.close()

if __name__ == "__main__":
    create_views_in_snowflake()
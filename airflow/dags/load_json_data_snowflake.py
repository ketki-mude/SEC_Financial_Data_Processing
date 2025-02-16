import os
import snowflake.connector
import logging
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Snowflake connection details and schema name
SNOWFLAKE_ACCOUNT   = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER      = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD  = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE  = os.getenv('SNOWFLAKE_DATABASE')
SCHEMA_NAME         = os.getenv('SCHEMA_NAME', 'SEC_JSON_DATA')

# AWS S3 and IAM details
AWS_ROLE_ARN        = os.getenv('AWS_ROLE_ARN')
AWS_S3_BUCKET_NAME  = os.getenv('AWS_S3_BUCKET_NAME')

# Initialize S3 Client
s3_client = boto3.client('s3')

def get_latest_s3_folder():
    """
    Fetch the latest available year and quarter folder in S3.
    Assumes folders are stored under a prefix (e.g., "JSON_Conversion/") with structure:
    JSON_Conversion/<year>/<quarter>/
    """
    prefix = "JSON_Conversion/"
    
    result = s3_client.list_objects_v2(Bucket=AWS_S3_BUCKET_NAME,
                                        Prefix=prefix, Delimiter='/')
    
    available_years = [obj['Prefix'].split('/')[-2]
                       for obj in result.get('CommonPrefixes', [])]
    if not available_years:
        raise ValueError("❌ No year folders found in S3 under JSON_Conversion!")
    
    latest_year = sorted(available_years, reverse=True)[0]
    
    year_prefix = f"{prefix}{latest_year}/"
    result = s3_client.list_objects_v2(Bucket=AWS_S3_BUCKET_NAME,
                                        Prefix=year_prefix, Delimiter='/')
    available_quarters = [obj['Prefix'].split('/')[-2]
                          for obj in result.get('CommonPrefixes', [])]
    
    if not available_quarters:
        raise ValueError(f"❌ No quarter folders found in {latest_year}!")
    
    latest_quarter = sorted(available_quarters, reverse=True)[0]
    logger.info(f"✅ Latest data found: {latest_year}/{latest_quarter}")
    return latest_year, latest_quarter

def get_all_json_files(latest_year, latest_quarter):
    """
    Get all `.json` files inside the latest year & quarter directory.
    """
    s3_prefix = f"JSON_Conversion/{latest_year}/{latest_quarter}/"
    result = s3_client.list_objects_v2(Bucket=AWS_S3_BUCKET_NAME, Prefix=s3_prefix)
    json_files = [obj['Key'] for obj in result.get('Contents', [])
                  if obj['Key'].endswith('.json')]
    if not json_files:
        raise ValueError(f"❌ No .json files found in {s3_prefix}!")
    
    logger.info(f"✅ Found {len(json_files)} JSON batch files.")
    return json_files

# --- Data Loading Function ---
def load_json_data_to_snowflake():
    try:
        # Get latest S3 folder details
        latest_year, latest_quarter = get_latest_s3_folder()
        json_files = get_all_json_files(latest_year, latest_quarter)

        # Build dynamic names for table and stage.
        # Expected table naming convention: <SCHEMA_NAME>.sec_data_<latest_year>_<latest_quarter>
        table_name = f"{SCHEMA_NAME}.sec_data_{latest_year}_{latest_quarter}"
        stage_name = f"sec_data_{latest_year}_{latest_quarter}"  # for external stage

        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE
        )
        cs = ctx.cursor()
        logger.info("✅ Successfully connected to Snowflake.")

        # Set active schema
        logger.info("🔹 Setting active schema...")
        cs.execute(f"USE SCHEMA {SCHEMA_NAME};")

        # Create Storage Integration
        logger.info("🔹 Creating storage integration...")
        CREATE_STORAGE_INTEGRATION_SQL = f"""
        CREATE OR REPLACE STORAGE INTEGRATION Snowflake_AWS_OBJ
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'S3'
            ENABLED = TRUE
            STORAGE_AWS_ROLE_ARN = '{AWS_ROLE_ARN}'
            STORAGE_ALLOWED_LOCATIONS = ('s3://{AWS_S3_BUCKET_NAME}/JSON_Conversion/');
        """
        cs.execute(CREATE_STORAGE_INTEGRATION_SQL)

        # Create External Stage with dynamic name
        logger.info("🔹 Creating external stage...")
        CREATE_STAGE_SQL = f"""
        CREATE OR REPLACE STAGE {stage_name}
            STORAGE_INTEGRATION = Snowflake_AWS_OBJ
            URL = 's3://{AWS_S3_BUCKET_NAME}/JSON_Conversion/'
            FILE_FORMAT = (TYPE = 'JSON');
        """
        cs.execute(CREATE_STAGE_SQL)

        # Create Table (with structured columns; year and quarter omitted)
        logger.info("🔹 Creating table with structured columns (without year and quarter)...")
        CREATE_TABLE_SQL = f"""
        CREATE OR REPLACE TABLE {table_name} (
            symbol STRING,
            company_name STRING,
            start_date DATE,
            end_date DATE,
            raw_json VARIANT
        );
        """
        cs.execute(CREATE_TABLE_SQL)

        # Truncate Table (clear out old data)
        logger.info("🔹 Truncating table to remove old records...")
        cs.execute(f"TRUNCATE TABLE {table_name};")

        # Copy JSON Data from S3 into the table
        logger.info(f"🔹 Copying JSON data from {len(json_files)} files into {table_name}...")
        COPY_INTO_SQL = f"""
        COPY INTO {table_name} (raw_json)
        FROM @{stage_name}/{latest_year}/{latest_quarter}/
        FILE_FORMAT = (TYPE = 'JSON')
        PATTERN = '.*\\.json';
        """
        cs.execute(COPY_INTO_SQL)

        # Extract fields from JSON into structured columns
        logger.info("🔹 Extracting JSON fields into structured columns...")
        UPDATE_TABLE_SQL = f"""
        UPDATE {table_name}
        SET 
            symbol = raw_json:"symbol"::STRING,
            company_name = raw_json:"name"::STRING,
            start_date = raw_json:"startDate"::DATE,
            end_date = raw_json:"endDate"::DATE;
        """
        cs.execute(UPDATE_TABLE_SQL)

        # Merge to avoid duplicates
        logger.info("🔹 Merging data to avoid duplicates...")
        MERGE_SQL = f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT 
                raw_json, 
                raw_json:"symbol"::STRING AS symbol,
                raw_json:"name"::STRING AS company_name,
                raw_json:"startDate"::DATE AS start_date,
                raw_json:"endDate"::DATE AS end_date
            FROM {table_name}
        ) AS source
        ON target.symbol = source.symbol
        WHEN MATCHED THEN 
            UPDATE SET 
                target.raw_json = source.raw_json,
                target.company_name = source.company_name,
                target.start_date = source.start_date,
                target.end_date = source.end_date
        WHEN NOT MATCHED THEN 
            INSERT (symbol, company_name, start_date, end_date, raw_json)
            VALUES (source.symbol, source.company_name, source.start_date, source.end_date, source.raw_json);
        """
        cs.execute(MERGE_SQL)

        # Check number of rows loaded
        cs.execute(f"SELECT COUNT(*) FROM {table_name};")
        row = cs.fetchone()
        row_count = row[0] if row else 0
        logger.info(f"✅ Data load and merge completed. Total rows loaded in {table_name}: {row_count}")

        # If data exists, create the views with dynamic names.
        if row_count > 0:
            create_views(ctx, table_name, latest_year, latest_quarter)
        else:
            logger.info("No data loaded; skipping view creation.")

    except Exception as e:
        logger.error(f"❌ Error loading JSON data into Snowflake: {e}", exc_info=True)
    finally:
        cs.close()
        ctx.close()

def create_views(ctx, table_name, latest_year, latest_quarter):
    """
    Creates views based on the loaded table.
    The views will extract JSON fields using LATERAL FLATTEN.
    The view names are dynamically suffixed with the latest year and quarter.
    """
    view_balance_sheet = f"{SCHEMA_NAME}.view_balance_sheet_{latest_year}_{latest_quarter}"
    view_income_statement = f"{SCHEMA_NAME}.view_income_statement_{latest_year}_{latest_quarter}"
    view_cash_flow = f"{SCHEMA_NAME}.view_cash_flow_{latest_year}_{latest_quarter}"

    CREATE_BALANCE_SHEET_VIEW_SQL = f"""
    CREATE OR REPLACE VIEW {view_balance_sheet} AS
    SELECT
        s.symbol,
        s.company_name,
        value:label::VARCHAR AS label,
        value:concept::VARCHAR AS concept,
        value:info::VARCHAR AS info,
        value:unit::VARCHAR AS unit,
        value:value::FLOAT AS value
    FROM {table_name} s,
    LATERAL FLATTEN(input => s.raw_json:data:bs) AS value;
    """

    CREATE_INCOME_STATEMENT_VIEW_SQL = f"""
    CREATE OR REPLACE VIEW {view_income_statement} AS
    SELECT
        s.symbol,
        s.company_name,
        value:label::VARCHAR AS label,
        value:concept::VARCHAR AS concept,
        value:info::VARCHAR AS info,
        value:unit::VARCHAR AS unit,
        value:value::FLOAT AS value
    FROM {table_name} s,
    LATERAL FLATTEN(input => s.raw_json:data:ic) AS value;
    """

    CREATE_CASH_FLOW_VIEW_SQL = f"""
    CREATE OR REPLACE VIEW {view_cash_flow} AS
    SELECT
        s.symbol,
        s.company_name,
        value:label::VARCHAR AS label,
        value:concept::VARCHAR AS concept,
        value:info::VARCHAR AS info,
        value:unit::VARCHAR AS unit,
        value:value::FLOAT AS value
    FROM {table_name} s,
    LATERAL FLATTEN(input => s.raw_json:data:cf) AS value;
    """

    try:
        cs = ctx.cursor()
        logger.info("✅ Creating view for balance sheet...")
        cs.execute(CREATE_BALANCE_SHEET_VIEW_SQL)
        logger.info("✅ Creating view for income statement...")
        cs.execute(CREATE_INCOME_STATEMENT_VIEW_SQL)
        logger.info("✅ Creating view for cash flow...")
        cs.execute(CREATE_CASH_FLOW_VIEW_SQL)
        logger.info("✅ Views created successfully.")
    except Exception as e:
        logger.error(f"❌ Error creating views: {e}", exc_info=True)
    finally:
        cs.close()

if __name__ == "__main__":
    load_json_data_to_snowflake()

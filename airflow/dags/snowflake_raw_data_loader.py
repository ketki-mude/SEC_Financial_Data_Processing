import os
import snowflake.connector
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SnowflakeLoader:
    def __init__(self, year, quarter):
        """Initialize Snowflake connection and configurations"""
        self.year = year
        self.quarter = quarter
        self.source_id = f"{year}Q{quarter}"
        
        self.conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        self.cur = self.conn.cursor()
        self.schema_name = f"SEC_DATA_RAW"
        self.s3_bucket = os.getenv('AWS_S3_BUCKET_NAME')
        self.s3_path = f'extracted/{self.source_id}/'  
        
        # AWS credentials for Snowflake integration
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
    def create_schema(self):
        """Create schema for raw SEC data"""
        try:
            # Drop schema if exists and create new one
            self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            self.cur.execute(f"USE SCHEMA {self.schema_name}")
            logger.info(f"Created and switched to schema: {self.schema_name}")
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise

    def create_tables(self):
        """Drop and then Create the required tables in Snowflake"""
        # Define table names with dynamic suffix
        table_suffix = f"_{self.source_id}"
        
        drop_statements = [
            f"DROP TABLE IF EXISTS sec_num{table_suffix}",
            f"DROP TABLE IF EXISTS sec_pre{table_suffix}",
            f"DROP TABLE IF EXISTS sec_sub{table_suffix}",
            f"DROP TABLE IF EXISTS sec_tag{table_suffix}"
        ]

        for drop_stmt in drop_statements:
            logger.info(f"Executing: {drop_stmt}")
            self.cur.execute(drop_stmt)

        tables_phase1 = {
            f"sec_tag{table_suffix}": f"""
                CREATE OR REPLACE TABLE sec_tag{table_suffix} (
                    tag VARCHAR(256) NOT NULL,
                    version VARCHAR(20) NOT NULL,
                    custom NUMBER(1,0),
                    abstract NUMBER(1,0),
                    datatype VARCHAR(20),
                    iord CHAR(1),
                    crdr CHAR(1),
                    tlabel VARCHAR(512),
                    doc TEXT,
                    source_file VARCHAR(20),
                    PRIMARY KEY (tag, version)
                );
            """,
            f"sec_sub{table_suffix}": f"""
                CREATE OR REPLACE TABLE sec_sub{table_suffix} (
                    adsh VARCHAR(20) NOT NULL,
                    cik NUMBER(38,0),
                    name VARCHAR(150),
                    sic NUMBER(38,0),
                    countryba CHAR(2),
                    stprba CHAR(2),
                    cityba VARCHAR(30),
                    zipba VARCHAR(10),
                    bas1 VARCHAR(40),
                    bas2 VARCHAR(40),
                    baph VARCHAR(20),
                    countryma CHAR(2),
                    stprma CHAR(2),
                    cityma VARCHAR(30),
                    zipma VARCHAR(10),
                    mas1 VARCHAR(40),
                    mas2 VARCHAR(40),
                    countryinc CHAR(3),
                    stprinc CHAR(2),
                    ein NUMBER(38,0),
                    former VARCHAR(150),
                    changed NUMBER(38,0),
                    afs VARCHAR(5),
                    wksi NUMBER(1,0),
                    fye NUMBER(38,0),
                    form VARCHAR(10),
                    period NUMBER(38,0),
                    fy NUMBER(38,0),
                    fp VARCHAR(2),
                    filed NUMBER(38,0),
                    accepted VARCHAR(30),
                    prevrpt NUMBER(1,0),
                    detail NUMBER(1,0),
                    instance VARCHAR(40),
                    nciks NUMBER(38,0),
                    aciks VARCHAR(120),
                    source_file VARCHAR(20),
                    PRIMARY KEY (adsh)
                );
            """
        }

        tables_phase2 = {
            f"sec_num{table_suffix}": f"""
                CREATE OR REPLACE TABLE sec_num{table_suffix} (
                    adsh VARCHAR(20) NOT NULL,
                    tag VARCHAR(256) NOT NULL,
                    version VARCHAR(20),
                    ddate NUMBER(38,0),
                    qtrs NUMBER(38,0),
                    uom VARCHAR(20),
                    segments VARCHAR,
                    coreg VARCHAR(256),
                    value NUMBER(38,10),
                    footnote VARCHAR(512),
                    source_file VARCHAR(20),
                    FOREIGN KEY (adsh) REFERENCES sec_sub{table_suffix}(adsh),
                    FOREIGN KEY (tag, version) REFERENCES sec_tag{table_suffix}(tag, version)
                );
            """,
            f"sec_pre{table_suffix}": f"""
                CREATE OR REPLACE TABLE sec_pre{table_suffix} (
                    adsh VARCHAR(20) NOT NULL,
                    report NUMBER(38,0),
                    line NUMBER(38,0),
                    stmt CHAR(2),
                    inpth NUMBER(1,0),
                    rfile CHAR(1),
                    tag VARCHAR(256) NOT NULL,
                    version VARCHAR(20) NOT NULL,
                    plabel VARCHAR(512),
                    negating NUMBER(1,0),
                    source_file VARCHAR(20),
                    FOREIGN KEY (adsh) REFERENCES sec_sub{table_suffix}(adsh),
                    FOREIGN KEY (tag, version) REFERENCES sec_tag{table_suffix}(tag, version)
                );
            """
        }

        # Create tables in correct order
        for table_name, ddl in tables_phase1.items():
            logger.info(f"Creating table: {table_name}")
            self.cur.execute(ddl)

        for table_name, ddl in tables_phase2.items():
            logger.info(f"Creating table: {table_name}")
            self.cur.execute(ddl)

    def setup_s3_integration(self):
        """Setup Snowflake integration with S3"""
        # Create storage integration
        integration_command = f"""
        CREATE OR REPLACE STORAGE INTEGRATION Snowflake_AWS_OBJ
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'S3'
            ENABLED = TRUE
            STORAGE_AWS_ROLE_ARN = '{os.getenv('AWS_ROLE_ARN')}'
            STORAGE_ALLOWED_LOCATIONS = ('s3://{self.s3_bucket}')
        """
        self.cur.execute(integration_command)

        # Get the AWS IAM user for the integration
        desc_integration_command = "DESC INTEGRATION Snowflake_AWS_OBJ"
        self.cur.execute(desc_integration_command)
        integration_info = self.cur.fetchall()
        logger.info("Integration Info:")
        for info in integration_info:
            logger.info(str(info))

        # Create file format for Parquet
        file_format_command = """
        CREATE OR REPLACE FILE FORMAT parquet_format
            TYPE = PARQUET
            COMPRESSION = 'SNAPPY'
        """
        self.cur.execute(file_format_command)

        # Create stage with dynamic name
        stage_name = f"sec_stage_{self.source_id}"
        stage_command = f"""
        CREATE OR REPLACE STAGE {stage_name}
            STORAGE_INTEGRATION = Snowflake_AWS_OBJ
            URL = 's3://{self.s3_bucket}/extracted/{self.source_id}/'
            FILE_FORMAT = parquet_format
        """
        self.cur.execute(stage_command)
        logger.info(f"Stage created for {self.source_id}")

    def load_data(self):
        """Load data from S3 parquet files into Snowflake tables"""
        table_suffix = f"_{self.source_id}"
        stage_name = f"sec_stage_{self.source_id}"
        
        file_table_mapping = {
            'sub': f'sec_sub{table_suffix}',
            'tag': f'sec_tag{table_suffix}',
            'num': f'sec_num{table_suffix}',
            'pre': f'sec_pre{table_suffix}'
        }

        for file_type, table_name in file_table_mapping.items():
            logger.info(f"\nProcessing {file_type}.parquet into {table_name}")
            
            file_check = f"SELECT COUNT(*) FROM @{stage_name}/{file_type}.parquet"
            try:
                self.cur.execute(file_check)
                file_count = self.cur.fetchone()[0]
                logger.info(f"File check count for {file_type}.parquet: {file_count}")
            except Exception as e:
                logger.error(f"Error checking file {file_type}.parquet: {str(e)}")
                continue
            
            copy_command = f"""
            COPY INTO {table_name}
            FROM @{stage_name}/{file_type}.parquet
            FILE_FORMAT = parquet_format
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE;
            """
            
            logger.info(f"Loading data into {table_name}")
            self.cur.execute(copy_command)
            
            # Verify row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            row_count = self.cur.execute(count_query).fetchone()[0]
            logger.info(f"Loaded {row_count} rows into {table_name}")

    def cleanup(self):
        """Close connections"""
        self.cur.close()
        self.conn.close()
        logger.info("Connections closed")
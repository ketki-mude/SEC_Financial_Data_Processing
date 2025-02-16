import boto3
import os
import io
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class SECDataProcessor:
    def __init__(self):
        """Initialize the processor with configurations"""
        self.s3_client = boto3.client('s3')
        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        
        # S3 paths
        self.RAW_PREFIX = 'raw/'
        self.EXTRACTED_PREFIX = 'extracted/'
        
        # File configurations
        self.FILE_TYPES = ['sub.txt', 'pre.txt', 'tag.txt', 'num.txt']
        
        # Expected headers (as defined in your original code)
        self.EXPECTED_HEADERS = {
            'sub.txt': ['adsh', 'cik', 'name', 'sic', 'countryba', 'stprba', 'cityba', 'zipba', 'bas1', 'bas2', 
                'baph', 'countryma', 'stprma', 'cityma', 'zipma', 'mas1', 'mas2', 'countryinc', 'stprinc',
                'ein', 'former', 'changed', 'afs', 'wksi', 'fye', 'form', 'period', 'fy', 'fp', 'filed',
                'accepted', 'prevrpt', 'detail', 'instance', 'nciks', 'aciks'],
            'pre.txt': ['adsh', 'report', 'line', 'stmt', 'inpth', 'rfile', 'tag', 'version', 'plabel', 'negating'],
            'tag.txt': ['tag', 'version', 'custom', 'abstract', 'datatype', 'iord', 'crdr', 'tlabel', 'doc'],
            'num.txt': ['adsh', 'tag', 'version', 'ddate', 'qtrs', 'uom', 'value', 'footnote', 'coreg']
        }

    def _standardize_data_types(self, df, file_name):
        """Standardize data types for each file type"""
        # Complete type mappings for all columns
        type_mappings = {
            'sub.txt': {
                # String columns
                'adsh': 'string',
                'name': 'string',
                'countryba': 'string',
                'stprba': 'string',
                'cityba': 'string',
                'zipba': 'string',
                'bas1': 'string',
                'bas2': 'string',
                'baph': 'string',
                'countryma': 'string',
                'stprma': 'string',
                'cityma': 'string',
                'zipma': 'string',
                'mas1': 'string',
                'mas2': 'string',
                'countryinc': 'string',
                'stprinc': 'string',
                'former': 'string',
                'afs': 'string',
                'form': 'string',
                'fp': 'string',
                'accepted': 'string',
                'instance': 'string',
                'aciks': 'string',
                # Numeric columns
                'cik': 'Int64',
                'sic': 'Float64',
                'ein': 'Int64',
                'changed': 'Float64',
                'wksi': 'Int64',
                'fye': 'Float64',
                'period': 'Int64',
                'fy': 'Float64',
                'filed': 'Int64',
                'prevrpt': 'Int64',
                'detail': 'Int64',
                'nciks': 'Int64'
            },
            'pre.txt': {
                # String columns
                'adsh': 'string',
                'stmt': 'string',
                'rfile': 'string',
                'tag': 'string',
                'version': 'string',
                'plabel': 'string',
                # Numeric columns
                'report': 'Int64',
                'line': 'Int64',
                'inpth': 'Int64',
                'negating': 'Int64'
            },
            'num.txt': {
                # String columns
                'adsh': 'string',
                'tag': 'string',
                'version': 'string',
                'uom': 'string',
                'coreg': 'string',
                'footnote': 'string',
                # Numeric columns
                'ddate': 'Int64',
                'qtrs': 'Int64',
                'value': 'Float64'
            },
            'tag.txt': {
                # String columns
                'tag': 'string',
                'version': 'string',
                'datatype': 'string',
                'iord': 'string',
                'crdr': 'string',
                'tlabel': 'string',
                'doc': 'string',
                # Numeric columns
                'custom': 'Int64',
                'abstract': 'Int64'
            }
        }

        logger.info(f"Starting data type standardization for {file_name}")
        logger.info(f"Original dtypes:\n{df.dtypes}")

        # Apply type mappings for all columns
        if file_name in type_mappings:
            for col, dtype in type_mappings[file_name].items():
                if col in df.columns:
                    try:
                        logger.info(f"Converting {col} to {dtype}")
                        if dtype in ['Int64', 'Float64']:
                            # For numeric types, use pd.to_numeric first
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
                        else:
                            # For string type
                            df[col] = df[col].astype(dtype)
                    except Exception as e:
                        logger.error(f"Error converting {col} to {dtype}: {str(e)}")
                        logger.error(f"Sample values: {df[col].head()}")
                        raise

        # Add source_file as string
        df['source_file'] = df['source_file'].astype('string')

        logger.info(f"Final dtypes after standardization:\n{df.dtypes}")
        
        # Verify all columns have been converted
        for col in df.columns:
            if col not in type_mappings[file_name] and col != 'source_file':
                logger.warning(f"Column {col} not included in type mappings!")

        return df

    def extract_zip_file(self, year, quarter):
        """Extract specific ZIP file and convert to parquet format"""
        try:
            # Construct the specific ZIP file key
            zip_key = f"{self.RAW_PREFIX}{year}_Q{quarter}.zip"
            source_id = f"{year}Q{quarter}"
            
            logger.info(f"Processing ZIP file: {zip_key}")
            
            try:
                # Download specific ZIP file
                zip_obj = self.s3_client.get_object(
                    Bucket=self.bucket_name, 
                    Key=zip_key
                )
            except self.s3_client.exceptions.NoSuchKey:
                logger.error(f"ZIP file not found: {zip_key}")
                raise FileNotFoundError(f"ZIP file not found for {year} Q{quarter}")
            
            # Process the ZIP file
            with zipfile.ZipFile(io.BytesIO(zip_obj['Body'].read())) as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name in self.FILE_TYPES:
                        self._process_zip_file(zip_ref, file_name, source_id)
                            
            logger.info(f"ZIP extraction completed successfully for {year} Q{quarter}")
            
        except Exception as e:
            logger.error(f"Error in extract_zip_file: {str(e)}")
            raise

    def _process_zip_file(self, zip_ref, file_name, source_id):
        """Process individual file from ZIP and convert to parquet"""
        try:
            with zip_ref.open(file_name) as file:
                # Read the CSV
                df = pd.read_csv(io.BytesIO(file.read()), sep='\t', low_memory=False)
                
                # Remove any duplicate columns
                df = df.loc[:, ~df.columns.duplicated()]
                
                # Add source information
                df['source_file'] = source_id

                # Standardize data types
                df = self._standardize_data_types(df, file_name)
                
                # Convert to parquet
                table = pa.Table.from_pandas(df)
                buffer = io.BytesIO()
                pq.write_table(
                    table, 
                    buffer, 
                    compression='snappy',
                    use_dictionary=True,
                    use_byte_stream_split=True
                )
                
                # Define path for parquet file
                file_type = file_name.replace('.txt', '')
                parquet_path = f"{self.EXTRACTED_PREFIX}{source_id}/{file_type}.parquet"
                
                # Upload to S3
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=parquet_path,
                    Body=buffer.getvalue()
                )
                
                logger.info(f"Processed and uploaded: {parquet_path}")
                    
        except Exception as e:
            logger.error(f"Error processing {file_name}: {str(e)}")
            raise
    
    # def consolidate_parquet_files(self):
    #     """Consolidate individual parquet files into final files"""
    #     try:
    #         for file_type in self.FILE_TYPES:
    #             type_prefix = file_type.replace('.txt', '')
    #             source_prefix = f"{self.CONSOLIDATED_PREFIX}{type_prefix}/"
                
    #             # List all parquet files for this type
    #             response = self.s3_client.list_objects_v2(
    #                 Bucket=self.bucket_name,
    #                 Prefix=source_prefix
    #             )
                
    #             if 'Contents' not in response:
    #                 logger.warning(f"No files found for {type_prefix}")
    #                 continue
                
    #             # Read and concatenate all parquet files
    #             dfs = []
    #             for obj in response['Contents']:
    #                 logger.info(f"Reading file: {obj['Key']}")
    #                 parquet_obj = self.s3_client.get_object(
    #                     Bucket=self.bucket_name,
    #                     Key=obj['Key']
    #                 )
    #                 df = pd.read_parquet(io.BytesIO(parquet_obj['Body'].read()))
    #                 logger.info(f"File columns: {df.columns.tolist()}")
    #                 logger.info(f"File dtypes:\n{df.dtypes}")
    #                 logger.info(f"File shape: {df.shape}")
    #                 dfs.append(df)
                
    #             if dfs:
    #                 # Concatenate all DataFrames
    #                 final_df = pd.concat(dfs, ignore_index=True)
    #                 logger.info(f"\nFinal consolidated DataFrame for {type_prefix}:")
    #                 logger.info(f"Columns: {final_df.columns.tolist()}")
    #                 logger.info(f"dtypes:\n{final_df.dtypes}")
    #                 logger.info(f"Shape: {final_df.shape}")
                    
    #                 # Convert to parquet
    #                 table = pa.Table.from_pandas(final_df)
    #                 logger.info(f"PyArrow table schema:\n{table.schema}")
                    
    #                 buffer = io.BytesIO()
    #                 pq.write_table(
    #                     table,
    #                     buffer,
    #                     compression='snappy',
    #                     use_dictionary=True,
    #                     use_byte_stream_split=True
    #                 )
                    
    #                 # Upload final consolidated file
    #                 final_path = f"{self.CONSOLIDATED_PREFIX}{type_prefix}.parquet"
    #                 self.s3_client.put_object(
    #                     Bucket=self.bucket_name,
    #                     Key=final_path,
    #                     Body=buffer.getvalue()
    #                 )
                    
    #                 logger.info(f"Created consolidated file: {final_path}")
                    
    #                 # Clean up individual parquet files
    #                 self._cleanup_individual_files(source_prefix)
                    
    #     except Exception as e:
    #         logger.error(f"Error in consolidate_parquet_files: {str(e)}")
    #         raise

    # def _cleanup_individual_files(self, prefix):
    #     """Clean up intermediate files"""
    #     try:
    #         response = self.s3_client.list_objects_v2(
    #             Bucket=self.bucket_name,
    #             Prefix=prefix
    #         )
            
    #         if 'Contents' in response:
    #             for obj in response['Contents']:
    #                 self.s3_client.delete_object(
    #                     Bucket=self.bucket_name,
    #                     Key=obj['Key']
    #                 )
                    
    #         logger.info(f"Cleaned up intermediate files in {prefix}")
            
    #     except Exception as e:
    #         logger.error(f"Error in cleanup: {str(e)}")
    #         raise

def main():
    """Main execution function"""
    try:
        year, quarter = 2023, 4
        processor = SECDataProcessor()
        processor.extract_zip_file(year, quarter)
        # processor.consolidate_parquet_files()
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
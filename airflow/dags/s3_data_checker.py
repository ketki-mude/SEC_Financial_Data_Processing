import os
import boto3
from dotenv import load_dotenv

load_dotenv()

def is_data_present_in_s3(year, quarter):
    """Checks if data is present in S3 for the given year and quarter."""
    s3 = boto3.client('s3')
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
    prefix = f'extracted/{year}Q{quarter}/'
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return 'Contents' in response
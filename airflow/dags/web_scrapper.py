import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import boto3
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

# URL of the page containing the data links
BASE_URL = 'https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets'

# AWS S3 bucket details
S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
S3_PREFIX = 'raw/'

# Custom headers to comply with SEC guidelines
HEADERS = {
    "User-Agent": "Northeastern University mutha.sa@northeastern.edu",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}

# Initialize S3 client
s3_client = boto3.client('s3')

def upload_to_s3(file_data, s3_path):
    """Upload file data to S3."""
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_path, Body=file_data)
    print(f"Successfully uploaded data to S3: {s3_path}")

def download_quarterly_data(year, quarter=None):
    """
    Download quarterly data for a specific year and optionally a specific quarter.
    """
    print(f"Downloading data for year {year}{f', quarter {quarter}' if quarter else ''}...")
    response = requests.get(BASE_URL, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to retrieve the page: {response.status_code}")
        return False

    soup = BeautifulSoup(response.text, 'html.parser')
    success = True
    downloads_count = 0

    # Find all links to ZIP files
    for link in soup.find_all('a', href=True):
        href = link['href']
        link_text = link.get_text().strip()
        
        # Check if the link matches our criteria
        if href.endswith('.zip') and str(year) in link_text:
            current_quarter = link_text.split()[1]  # Extract quarter information
            
            # Skip if a specific quarter was requested and this isn't it
            if quarter and f"Q{quarter}" != current_quarter:
                continue

            file_url = urljoin(BASE_URL, href)
            s3_key = f"{S3_PREFIX}{year}_{current_quarter}.zip"

            try:
                response = requests.get(file_url, headers=HEADERS, stream=True)
                response.raise_for_status()  # Raise an exception for bad status codes

                # Prepare the ZIP file for upload
                zip_data = BytesIO()
                for chunk in response.iter_content(chunk_size=8192):
                    zip_data.write(chunk)
                zip_data.seek(0)

                # Upload to S3
                upload_to_s3(zip_data, s3_key)
                print(f"Successfully downloaded and uploaded {year} {current_quarter} data")
                downloads_count += 1

            except requests.exceptions.RequestException as e:
                print(f"Failed to download {file_url}: {str(e)}")
                success = False
            except Exception as e:
                print(f"Failed to process {year} {current_quarter} data: {str(e)}")
                success = False

    if downloads_count == 0:
        print(f"No data found for year {year}{f', quarter {quarter}' if quarter else ''}")
        return False

    return success

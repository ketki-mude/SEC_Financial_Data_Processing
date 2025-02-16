import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import boto3
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

BASE_URL = 'https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets'
S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME', "your_bucket_name")  # Default added
S3_PREFIX = 'raw/'

HEADERS = {
    "User-Agent": "Northeastern University mude.k@northeastern.edu (Ketki Mude)",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}

s3_client = boto3.client('s3')

def upload_to_s3(file_data, s3_path):
    """Upload file data to S3."""
    if not S3_BUCKET_NAME:
        print("Error: S3 bucket name is not set!")
        return
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_path, Body=file_data)
    print(f"Successfully uploaded data to S3: {s3_path}")

def download_quarterly_data(year, quarter=None):
    """Download SEC quarterly data"""
    response = requests.get(BASE_URL, headers=HEADERS)
    if response.status_code != 200:
        print(f"Failed to retrieve SEC page: {response.status_code}")
        return False

    soup = BeautifulSoup(response.text, 'html.parser')
    success = False
    downloads_count = 0

    for link in soup.find_all('a', href=True):
        href = link['href']
        link_text = link.get_text().strip()
        print(f"Found link: {link_text} -> {href}")  # Debugging line

        if href.endswith('.zip') and str(year) in link_text:
            current_quarter = link_text.split()[1]
            # Convert quarter to string and compare with current_quarter in uppercase
            if quarter and f"Q{quarter}" != current_quarter.upper():
                continue

            file_url = urljoin(BASE_URL, href)
            s3_key = f"{S3_PREFIX}{year}_{current_quarter}.zip"

            try:
                print(f"Downloading: {file_url}")
                zip_data = BytesIO(requests.get(file_url, headers=HEADERS, stream=True).content)

                if zip_data.getbuffer().nbytes == 0:
                    print("Error: ZIP file is empty!")
                    return False

                upload_to_s3(zip_data, s3_key)
                print(f"Uploaded {year} {current_quarter} data to S3.")
                downloads_count += 1
                success = True

            except requests.exceptions.RequestException as e:
                print(f"Failed to download {file_url}: {str(e)}")
            except Exception as e:
                print(f"Failed to process {year} {current_quarter} data: {str(e)}")

    if downloads_count == 0:
        print(f"No data found for {year} {f'Q{quarter}' if quarter else ''}")
        return False

    return success


if __name__ == "__main__":
    downloaded_data = download_quarterly_data("2024", "1")
    print("Download Status:", downloaded_data)

import boto3
import os
import io
import zipfile
import ujson  
import pandas as pd
import gc  # Force garbage collection
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 details
S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
RAW_PREFIX = 'raw/'

# Chunk size for reading large CSV files
CHUNK_SIZE = 1000000

# ---------------------------
# Optimized S3 Upload
# ---------------------------
def upload_to_s3(file_data, s3_path):
    """
    Upload JSON directly to S3 in correct folder structure (JSON_conversion/2024/q1/filename.json).
    """
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_path, Body=file_data)
    print(f"✅ Uploaded JSON: {s3_path}")

# ---------------------------
# JSON Processing Worker
# ---------------------------
def process_submission(adsh, cik, period, fp, fy, name, countryma, cityma, zip_path, year, quarter):
    """
    Unzips, converts to JSON, and uploads to S3.
    """
    try:
        print(f"✅ Processing: {adsh}")

        # Open the ZIP file and read the files using chunking
        with zipfile.ZipFile(zip_path, 'r') as myzip:
            # Read num.txt in chunks (iterator)
            dfNum_iter = pd.read_csv(
                myzip.open('num.txt'),
                delimiter="\t",
                low_memory=False,
                chunksize=CHUNK_SIZE
            )
            # For pre.txt and tag.txt, load chunks into lists so we can iterate repeatedly
            dfPre_chunks = list(pd.read_csv(
                myzip.open('pre.txt'),
                delimiter="\t",
                low_memory=False,
                chunksize=CHUNK_SIZE
            ))
            dfTag_chunks = list(pd.read_csv(
                myzip.open('tag.txt'),
                delimiter="\t",
                low_memory=False,
                chunksize=CHUNK_SIZE
            ))

        # Fetch ticker file from S3 (assuming the ticker file is stored under bucket "scrapedata")
        s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket="scrapedata", Key="ticker.txt")
        dfSym = pd.read_csv(
            io.BytesIO(obj['Body'].read()),
            delimiter="\t",
            header=None,
            names=['symbol', 'cik']
        )

        # Get symbol using cik from ticker file; default to "UNKNOWN" if not found
        symbol_info = dfSym.loc[dfSym["cik"] == cik, "symbol"]
        symbol = symbol_info.iloc[0] if not symbol_info.empty else "UNKNOWN"

        if pd.isna(period):
            print(f"⚠ Skipping {adsh}: NaN period")
            return

        try:
            period_str = str(int(float(period)))
            start_date = datetime.strptime(period_str, "%Y%m%d").strftime("%Y-%m-%d")
            end_date = start_date
        except Exception:
            print(f"⚠ Skipping {adsh}: Invalid period")
            return

        # Build base JSON structure
        financials_data = {
            "quarter": str(fp),
            "country": countryma if countryma else "UNKNOWN",
            "data": {"bs": [], "cf": [], "ic": []},
            "year": int(fy) if not pd.isna(fy) else 0,
            "name": name,
            "startDate": start_date,
            "endDate": end_date,
            "symbol": symbol,
            "city": cityma if cityma else "UNKNOWN"
        }

        # Process dfNum in chunks
        for df_chunk in dfNum_iter:
            dfNumFiltered = df_chunk[df_chunk['adsh'] == adsh]
            for _, row in dfNumFiltered.iterrows():
                # Find label from dfTag (search in each chunk)
                label = "Unknown"
                for df_tag in dfTag_chunks:
                    if row["tag"] in df_tag["tag"].values:
                        label = df_tag.loc[df_tag["tag"] == row["tag"], "doc"].values[0]
                        break

                # Find info and statement type from dfPre (search in each chunk)
                info = "Unknown"
                stmt_type = "UNKNOWN"
                for df_pre in dfPre_chunks:
                    pre_filtered = df_pre[(df_pre["adsh"] == adsh) & (df_pre["tag"] == row["tag"])]
                    if not pre_filtered.empty:
                        info = pre_filtered["plabel"].values[0]
                        stmt_type = pre_filtered["stmt"].values[0]
                        break

                element = {
                    "label": label,
                    "concept": row["tag"],
                    "info": info,
                    "unit": row["uom"],
                    "value": row["value"] if not pd.isna(row["value"]) else 0
                }

                # Append the element into the correct statement category
                if stmt_type == "BS":
                    financials_data["data"]["bs"].append(element)
                elif stmt_type == "CF":
                    financials_data["data"]["cf"].append(element)
                elif stmt_type in ["IC", "IS"]:
                    financials_data["data"]["ic"].append(element)

        # Convert the financials_data dict to a JSON string
        json_str = ujson.dumps(financials_data)

        # Correct folder structure (JSON_Conversion/2024/q1/filename.json)
        s3_path_out = f"JSON_Conversion/{year}/q{quarter}/{adsh}.json"

        # Upload JSON directly to S3
        upload_to_s3(json_str, s3_path_out)

        # Free memory if needed
        del dfNum_iter, dfPre_chunks, dfTag_chunks, dfSym
        gc.collect()

    except Exception as ex:
        print(f"❌ Error processing {adsh}: {str(ex)}")

# ---------------------------
# Extract, Convert & Upload JSON Sequentially
# ---------------------------
def extract_and_convert_to_json(year, quarter):
    """Processes SEC reports sequentially."""
    zip_key = f"{RAW_PREFIX}{year}_Q{quarter}.zip"
    print(f"🔹 Downloading ZIP file from S3: {zip_key}")

    s3_client = boto3.client('s3')
    zip_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=zip_key)
    zip_path = f"{year}_Q{quarter}.zip"

    with open(zip_path, "wb") as f:
        f.write(zip_obj['Body'].read())

    # Read submission file from the ZIP
    with zipfile.ZipFile(zip_path, 'r') as myzip:
        dfSub = pd.read_csv(myzip.open('sub.txt'), delimiter="\t", low_memory=False)

    # Limit the number of files processed for testing (adjust as needed)
    # dfSub = dfSub.head(5)

    # Process each submission one after the other
    for _, row in dfSub.iterrows():
        process_submission(
            row.adsh,
            row.cik,
            row.period,
            row.fp,
            row.fy,
            row["name"],    # Use row["name"] to get the company name
            row.countryma,
            row.cityma,
            zip_path,
            year,
            quarter
        )

    print("✅ All JSON files processed and uploaded.")

if __name__ == "__main__":
    extract_and_convert_to_json(2024, 1)


import requests
import json
from datetime import date
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from azure.storage.blob import BlobServiceClient

# Configuration
API_URL = "https://api.product-usage.internal/v2/usage"
API_KEY = "your_secret_key" # Fetch from Key Vault in prod
BLOB_CONN_STR = "your_blob_conn_string"
CONTAINER = "bronze-zone"

# Retry Logic
# Retries up to 5 times, waiting 2, 4, 8... seconds between attempts. 
# Only retries on specific connection or server errors, not on 401 Unauthorized as retring at 401 must fail fast and retry at 400(bad request) useless.
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=30), 
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout))
)
def fetch_page(target_date: str, page: int):
    response = requests.get(
        url=API_URL,
        headers={"Authorization": f"Bearer {API_KEY}"},
        params={"date": target_date, "page": page, "page_size": 500},
        timeout=30
    )
    # Raise for 5xx errors to trigger retry
    if response.status_code >= 500:
        response.raise_for_status()
        
    return response.json()

# 2. Main Extraction
def extract_api_to_blob(target_date: date, run_id: str):
    date_str = target_date.isoformat()
    all_records = []
    page = 1
    
    # Handle Pagination
    while True:
        payload = fetch_page(date_str, page)
        all_records.extend(payload.get("data", []))
        
        if not payload.get("has_next_page", False):
            break
        page += 1
        
    if not all_records:
        print(f"[{run_id}] No records found for {date_str}.")
        return

    # 3. Land to Blob (Bronze Layer)
    # Path partitioning enables efficient downstream loading
    blob_path = f"raw_usage/year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/{run_id}.json"
    
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service.get_blob_client(container=CONTAINER, blob=blob_path)
    
    # Dump exactly as received for pure auditability
    blob_client.upload_blob(
        data=json.dumps(all_records), 
        overwrite=True
    )
    print(f"[{run_id}] Successfully landed {len(all_records)} records to {blob_path}.")

# Example Execution
# extract_api_to_blob(date(2026, 2, 2), "adf-run-xyz-123")

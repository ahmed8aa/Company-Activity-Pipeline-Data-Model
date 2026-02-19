Question 1:

1.1 - For target analytics table:
	The GRAIN : one row per (company,date) as we want a time series analytics and we can compute metrics like rolling avgs.

	The columns to be listed :	-- from the dialy crm --
					
					1- company_id
					2- company_name
					3- country (we can use to segment by country, making heat maps or feature in ML)
					4- industry_tag (future usage in Cohort analysis)
					5- last_contact_at ( can be used to measure  days_since_contact )
					
					-- from the staging product_usage --
					6- active_users (so we can derive metrics like 7d_active_users and is_churn_risk)
					7- events (for further dashboard analytics)
					8- activity_date

					-- derived metrics --
					9- rolling_7d_active_users
					10- is_churn_risk
					11- days_since_last_contact dervied from (last_contact_at)

					-- patching --
					12- inserted_at
					13- pipeline_run_id
					
-----------------------------------------------------------------------------------------------------------------------------------                                          
```
1.2 -  SQL (from sensible staging tables) to populate Analytics table :

- ASSUMPTION: I'm USING postgres SQL syntax

- INSERT INTO company_activity ( 
    company_id, activity_date, company_name, country, industry_tag,
    active_users, events, rolling_7d_active_users, days_since_last_contact, is_churn_risk,
    pipeline_run_id, inserted_at
)
WITH recent_7d_usage AS (
    -- Pull 7 days of data so the window function has enough rows to look back at.
    -- If we filter to just today here, the rolling sum would always equal today's value only.
    SELECT 
        company_id, 
        date AS activity_date, 
        active_users, 
        events
    FROM stg_product_usage
    WHERE date >= '@{pipeline().parameters.ProcessingDate}'::DATE - INTERVAL '6 days'
      AND date <= '@{pipeline().parameters.ProcessingDate}'::DATE
),
daily_usage_with_rolling AS (
    -- Compute the rolling 7-day sum over the full 7-day dataset.
    -- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW now has actual history to sum across.
    SELECT 
        company_id,
        activity_date,
        active_users,
        events,
        SUM(active_users) OVER (
            PARTITION BY company_id
            ORDER BY activity_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_active_users
    FROM recent_7d_usage
),
target_day_usage AS (
    -- Now that the window is computed correctly, isolate just the target day.
    -- This is the only row per company that will be inserted/upserted.
    SELECT * 
    FROM daily_usage_with_rolling
    WHERE activity_date = '@{pipeline().parameters.ProcessingDate}'::DATE
),
enriched_activity AS (
    SELECT
        u.company_id,
        u.activity_date,
        c.name          AS company_name,
        c.country,
        c.industry_tag,
        u.active_users,
        u.events,
        u.rolling_7d_active_users,
        -- PostgreSQL native date subtraction instead of DATEDIFF()
        -- gives a point-in-time view of how long it had been since contact.
        u.activity_date - c.last_contact_at::DATE AS days_since_last_contact
    FROM target_day_usage u
    LEFT JOIN stg_crm_companies c ON c.company_id = u.company_id
)
SELECT 
    company_id,
    activity_date,
    company_name,
    country,
    industry_tag,
    active_users,
    events,
    rolling_7d_active_users,
    days_since_last_contact,

    -- I choose the logic because churn risk isn't just low usage; it's low usage combined with lack of outreach.
    -- COALESCE handles NULL last_contact_at (no CRM record) treating it as never contacted,
    -- which is the highest risk case and should not be silently excluded.
    CASE 
        WHEN rolling_7d_active_users < 10 
         AND COALESCE(days_since_last_contact, 999) > 30 THEN TRUE
        ELSE FALSE 
    END AS is_churn_risk,

    '@{pipeline().RunId}'  AS pipeline_run_id,
    CURRENT_TIMESTAMP      AS inserted_at

FROM enriched_activity

-- Upsert strategy: if a record for the same company_id + activity_date already exists
-- (e.g. pipeline retry or manual re-run), overwrite the metrics with the latest values
-- rather than inserting a duplicate row. This ensures the pipeline is safely idempotent.
ON CONFLICT (company_id, activity_date) 
DO UPDATE SET
    company_name            = EXCLUDED.company_name,
    country                 = EXCLUDED.country,
    industry_tag            = EXCLUDED.industry_tag,
    active_users            = EXCLUDED.active_users,
    events                  = EXCLUDED.events,
    rolling_7d_active_users = EXCLUDED.rolling_7d_active_users,
    days_since_last_contact = EXCLUDED.days_since_last_contact,
    is_churn_risk           = EXCLUDED.is_churn_risk,
    inserted_at             = EXCLUDED.inserted_at,
    pipeline_run_id         = EXCLUDED.pipeline_run_id;
```
----------------------------------------------------------------------------------------------------------------------------------- 
```
1.3 - The ADF flow 




=====================================================================================
  TRIGGER: trg_daily_0034 (Passes @trigger().outputs.windowStartTime as target_date)
=====================================================================================
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 1. IDEMPOTENCY GATE                                                         │
  │ act_lookup_run (Lookup Activity)                                            │
  │ Queries pipeline_log: "Did we already process this target_date successfully?"│
  └───────────────────────────────────┬─────────────────────────────────────────┘
                                      │ (If No)
          ┌───────────────────────────┴───────────────────────────┐
          ▼                                                       ▼
  ┌──────────────────────────────┐                ┌──────────────────────────────┐
  │ 2A. API INGESTION (BRONZE)   │                │ 2B. CRM INGESTION (BRONZE)   │
  │ act_api_ingest               │ (Runs in       │ act_copy_crm                 │
  │ (Azure Function / Databricks)│  Parallel)     │ (Copy Data Activity)         │
  │ Executes Python script.      │                │ Moves CSV from Blob to       │
  │ Lands raw JSON to Blob.      │                │ Staging SQL / Synapse.       │
  └──────────────┬───────────────┘                └──────────────┬───────────────┘
                 │                                               │
                 └────────────────────────┬──────────────────────┘
                                          ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 3. STAGING LOAD (SILVER)                                                    │
  │ act_copy_api_to_sql (Copy Data Activity)                                    │
  │ Native ADF bulk-copy moves the raw JSON/Parquet from Blob -> stg_usage_daily│
  └───────────────────────────────────────┬─────────────────────────────────────┘
                                          ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 4. DATA QUALITY GATE                                                        │
  │ act_dq_checks (Script Activity / Stored Procedure)                          │
  │ Checks stg_usage_daily for: active_users >= 0, missing company_ids          │
  └───────────────────────────────────────┬─────────────────────────────────────┘
                                          │ (If Passed)
                                          ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 5. TRANSFORM & UPSERT (GOLD)                                                │
  │ act_transform_gold (Stored Procedure Activity)                              │
  │ Executes the complex CTE + ON CONFLICT DO UPDATE SQL script.                │
  └───────────────────────────────────────┬─────────────────────────────────────┘
                                          ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 6. SUCCESS LOGGING                                                          │
  │ act_log_success (Script Activity)                                           │
  │ Writes RunID, target_date, and success status to pipeline_log table.        │
  └─────────────────────────────────────────────────────────────────────────────┘

=====================================================================================
  GLOBAL ERROR HANDLING (Attached to the "On Failure" path of EVERY activity)
=====================================================================================
          │ (On Failure - Red Arrow)
          ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │ 🚨 FAILURE ALERTING                                                         │
  │ act_on_failure_alert (Web Activity)                                         │
  │ Triggers an Azure Logic App webhook.                                        │
  │ Sends a Microsoft Teams/Email alert containing:                             │
  │ - Pipeline Name                                                             │
  │ - Failed Activity Name                                                      │
  │ - Error Message                                                             │
  │ - Run ID (for easy debugging)                                               │
  └─────────────────────────────────────────────────────────────────────────────┘



Key Stages : 

- Idempotency Gate (Top): Sits at the very beginning to prevent wasting compute on runs that have already succeeded.

- API Ingestion (Parallel with CRM): The API extraction sits in the Bronze/Raw phase. We run it concurrently with the CRM CSV copy because they don't depend on each other. Network calls are slow, so running them in parallel cuts the total pipeline time in half.

- Data Quality Gate (Middle): Sits after staging but before the Gold transformation. You never want to run expensive calculation logic (like 7-day rolling sums) on garbage data, nor do you want corrupted data hitting the BI dashboard.

- Failure Alerting (Global): Rather than putting an alert at the very end of the pipeline, you attach a "Failure" path (red arrow in ADF UI) from every single activity pointing to one shared Web Activity. If the API fails, the CRM copy fails, or the SQL script fails, you get notified immediately.


Naming Approach (The Taxonomy):

To keep the ADF workspace organized and searchable as your team scales, strictly adhere to these prefixes:

Pipelines (pl_): Describes the business process.

Activities (act_): Describes the specific action being taken.

Datasets (ds_): Describes the format and location of the data.

Linked Services (ls_): Describes the connection to external systems.

Triggers (trg_): Describes the schedule or event.

```
----------------------------------------------------------------------------------------------------------------------------------- 
```
1.4 - python function for calling API:


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
# extract_api_to_blob(date(2023, 10, 27), "adf-run-xyz-123")

```
----------------------------------------------------------------------------------------------------------------------------------- 


1.5 -

	I would implement the API ingestion first because it is the only non-replayable, externally controlled dependency. 
	Missing that window risks permanent data loss. By landing raw data into Bronze, 
	I guarantee recoverability and allow all downstream layers (DQ, transformations, dashboards) to be rebuilt later. 
	I would explicitly postpone transformations, DQ gates, alerting, and orchestration because they are operational optimizations, 
	not data survival requirements.
	Even if downstream transformation fails, we at least preserve raw truth in Bronze, which allows full reprocessing.

	I prioritize whatever is hardest to recover from if missed — and a time-windowed external API call is always that thing.





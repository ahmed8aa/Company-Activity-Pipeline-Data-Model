
INSERT INTO company_activity ( 
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

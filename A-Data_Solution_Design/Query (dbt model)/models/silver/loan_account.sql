{{
  config(
    materialized = 'incremental',
    unique_key = 'loan_id',
    cluster_by = ["loan_id", "user_id"],
    schema = 'silver'
  )
}}

/*
  Deduplicates raw.loan_accounts as of report_date.
  Resolves latest state per loan_id with tie-breaker: PAID > OVERDUE > ACTIVE.
*/

WITH latest_loans AS (
    SELECT 
        l.loan_id,
        l.user_id,
        l.product_id AS product_type,
        l.status AS loan_status,
        l.principal_amount,
        l.disbursed_at,
        l.ingestion_date,
        ROW_NUMBER() OVER (
            PARTITION BY l.loan_id 
            ORDER BY 
                l.ingestion_date DESC,
                CASE l.status 
                    WHEN 'PAID' THEN 1 
                    WHEN 'OVERDUE' THEN 2 
                    WHEN 'ACTIVE' THEN 3 
                    ELSE 4 
                END ASC
        ) AS rn
    FROM {{ source('raw_monee', 'loan_accounts') }} l
    WHERE l.ingestion_date <= DATE('{{ var("report_date", (run_started_at - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d")) }}')
    {% if is_incremental() %}
      AND l.ingestion_date >= (SELECT MAX(ingestion_date) FROM {{ this }})
    {% endif %}
)

SELECT 
    loan_id,
    user_id,
    product_type,
    loan_status,
    principal_amount,
    disbursed_at,
    ingestion_date
FROM latest_loans
WHERE rn = 1

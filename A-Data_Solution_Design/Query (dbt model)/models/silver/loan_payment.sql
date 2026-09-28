{{
  config(
    materialized = 'incremental',
    unique_key = 'payment_id',
    cluster_by = ["loan_id"],
    schema = 'silver'
  )
}}

/*
  Deduplicates raw.loan_payments as of report_date.
  Extracts latest status per payment_id.
*/

WITH latest_payments AS (
    SELECT 
        p.payment_id,
        p.loan_id,
        p.payment_date,
        p.amount,
        p.payment_status,
        p.ingestion_date,
        ROW_NUMBER() OVER (
            PARTITION BY p.payment_id 
            ORDER BY p.ingestion_date DESC
        ) AS rn
    FROM {{ source('raw_monee', 'loan_payments') }} p
    WHERE p.ingestion_date <= DATE('{{ var("report_date", (run_started_at - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d")) }}')
    {% if is_incremental() %}
      AND p.ingestion_date >= (SELECT MAX(ingestion_date) FROM {{ this }})
    {% endif %}
)

SELECT 
    payment_id,
    loan_id,
    payment_date,
    amount,
    payment_status,
    ingestion_date
FROM latest_payments
WHERE rn = 1

{{
  config(
    materialized = 'incremental',
    unique_key = 'loan_id',
    cluster_by = ["loan_id", "user_id"],
    schema = 'gold'
  )
}}

/*
  Gold mart pre-joining loans, users, and aggregated successful payments.
  Serves as the canonical source for downstream regulatory reporting.
*/

WITH loans AS (
    SELECT * FROM {{ ref('loan_account') }}
),

users AS (
    SELECT * FROM {{ ref('dim_users') }}
),

successful_payments AS (
    SELECT 
        loan_id,
        SUM(amount) AS total_paid
    FROM {{ ref('loan_payment') }}
    WHERE payment_status = 'SUCCESS'
      AND payment_date <= DATE('{{ var("report_date", (run_started_at - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d")) }}')
    GROUP BY loan_id
)

SELECT 
    l.loan_id,
    l.user_id,
    u.name AS user_name,
    u.risk_segment,
    l.product_type,
    l.loan_status,
    l.principal_amount,
    l.disbursed_at,
    COALESCE(sp.total_paid, 0.0) AS total_paid,
    GREATEST(0.0, l.principal_amount - COALESCE(sp.total_paid, 0.0)) AS outstanding_principal
FROM loans l
LEFT JOIN users u ON l.user_id = u.user_id
LEFT JOIN successful_payments sp ON l.loan_id = sp.loan_id

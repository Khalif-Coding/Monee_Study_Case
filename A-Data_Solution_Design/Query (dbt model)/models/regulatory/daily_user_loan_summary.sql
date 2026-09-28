{{
  config(
    materialized = 'view',
    schema = 'regulatory'
  )
}}

/*
  Regulatory Output B: user-level exposure summary (grain: report_date x user_id).
*/

SELECT 
    DATE('{{ var("report_date", (run_started_at - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d")) }}') AS report_date,
    user_id,
    user_name AS name,
    risk_segment,
    COUNT(CASE WHEN loan_status = 'ACTIVE' THEN 1 END) AS active_loan_count,
    COUNT(CASE WHEN loan_status = 'OVERDUE' THEN 1 END) AS overdue_loan_count,
    SUM(principal_amount) AS total_principal,
    SUM(outstanding_principal) AS total_outstanding
FROM {{ ref('fact_loan_daily') }}
GROUP BY 
    user_id,
    user_name,
    risk_segment

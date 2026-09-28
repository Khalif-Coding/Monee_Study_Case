{{
  config(
    materialized = 'view',
    schema = 'regulatory'
  )
}}

/*
  Regulatory Output A: loan-level performance mart (grain: report_date x loan_id).
*/

SELECT 
    DATE('{{ var("report_date", (run_started_at - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d")) }}') AS report_date,
    loan_id,
    user_id,
    product_type,
    loan_status,
    principal_amount,
    total_paid,
    outstanding_principal
FROM {{ ref('fact_loan_daily') }}

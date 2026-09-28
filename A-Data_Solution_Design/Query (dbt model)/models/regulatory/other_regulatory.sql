{{
  config(
    materialized = 'view',
    schema = 'regulatory'
  )
}}

/*
  Regulatory Output C (Sample): SLIK NPL monitoring view filtering overdue loans from gold fact.
*/

SELECT 
    DATE('{{ var("report_date", (run_started_at - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d")) }}') AS report_date,
    loan_id,
    user_id,
    user_name,
    product_type,
    outstanding_principal AS npl_exposure,
    disbursed_at
FROM {{ ref('fact_loan_daily') }}
WHERE loan_status = 'OVERDUE'

/*
  Singular test: verifies total principal and outstanding match between Output A and Output B.
  Returns discrepancies (failing test if row count > 0).
*/

WITH total_output_a AS (
    SELECT 
        report_date,
        SUM(principal_amount) AS sum_principal_a,
        SUM(outstanding_principal) AS sum_outstanding_a
    FROM {{ ref('daily_loan_performance') }}
    GROUP BY report_date
),

total_output_b AS (
    SELECT 
        report_date,
        SUM(total_principal) AS sum_principal_b,
        SUM(total_outstanding) AS sum_outstanding_b
    FROM {{ ref('daily_user_loan_summary') }}
    GROUP BY report_date
)

SELECT 
    a.report_date,
    a.sum_principal_a,
    b.sum_principal_b,
    (a.sum_principal_a - b.sum_principal_b) AS diff_principal,
    a.sum_outstanding_a,
    b.sum_outstanding_b,
    (a.sum_outstanding_a - b.sum_outstanding_b) AS diff_outstanding
FROM total_output_a a
JOIN total_output_b b ON a.report_date = b.report_date
WHERE a.sum_principal_a != b.sum_principal_b
   OR a.sum_outstanding_a != b.sum_outstanding_b

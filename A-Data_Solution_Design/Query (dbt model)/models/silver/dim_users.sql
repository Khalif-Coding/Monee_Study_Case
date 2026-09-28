{{
  config(
    materialized = 'incremental',
    unique_key = 'user_id',
    cluster_by = ["user_id"],
    schema = 'silver'
  )
}}

/*
  Deduplicates raw.users to extract latest user profile version as of report_date.
*/

WITH latest_users AS (
    SELECT 
        u.user_id,
        u.name,
        u.date_of_birth,
        u.occupation,
        u.risk_segment,
        u.ingestion_date,
        ROW_NUMBER() OVER (
            PARTITION BY u.user_id 
            ORDER BY u.ingestion_date DESC
        ) AS rn
    FROM {{ source('raw_monee', 'users') }} u
    WHERE u.ingestion_date <= DATE('{{ var("report_date", (run_started_at - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d")) }}')
    {% if is_incremental() %}
      AND u.ingestion_date >= (SELECT MAX(ingestion_date) FROM {{ this }})
    {% endif %}
)

SELECT 
    user_id,
    name,
    date_of_birth,
    occupation,
    risk_segment,
    ingestion_date
FROM latest_users
WHERE rn = 1

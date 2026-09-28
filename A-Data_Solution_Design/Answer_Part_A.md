# Monee Data Engineering Take-Home Test
## Section A: Data Solution Design

**Target Environment:** Google Cloud Platform (BigQuery, dbt, Apache Airflow / Cloud Composer)  

## 1. Context & Problem Statement

A credit company must submit daily loan performance data to multiple regulatory portals. Source data lands in the `raw` schema of the multi-terabyte data warehouse, partitioned physically by `ingestion_date` (representing the warehouse landing/update date).

### Existing Pain Points:
1. **Duplicate Records & Multiple Versions:** Source tables (`raw.loan_accounts` [2 TB], `raw.loan_payments` [3 TB], and `raw.users` [50 GB]) operate under Change Data Capture (CDC) / append-only semantics, where a single `loan_id`, `payment_id`, or `user_id` contains multiple state versions over time.
2. **Slow Performance & High Scanning Costs:** Querying 5+ TB of raw data directly for every regulatory report results in full-table scans, massive slot consumption, and query queueing.
3. **Inconsistent Payment States:** Payment statuses evolve asynchronously (`SUCCESS`, `FAILED`, `REVERSED`), risking financial discrepancies if reversed or post-dated transactions are counted.
4. **Manual Submission:** Lack of automated orchestration leads to operational bottlenecks and human error.

---

## 2. Engineering Assumptions Established

To keep results deterministic and costs under control, these assumptions apply:

1. **CDC Append-Only Ingestion & Physical Partitioning:**
   * Source tables (`raw.loan_accounts`, `raw.loan_payments`, `raw.users`) are append-only.
   * `ingestion_date` is the physical partition key across all raw tables in BigQuery.
2. **Point-in-Time Snapshot (SCD Type 1 As-Of-Date):**
   * The valid profile or status of an entity for any given `report_date` (T) is the latest version ingested on or before T:
     `WHERE ingestion_date <= T` then `ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_date DESC) = 1`
3. **Same-Day Status Resolution (Deterministic Tie-Breaker Rule):**
   * When an entity undergoes multiple state transitions within the exact same `ingestion_date` partition (as observed in PDF Page 2 for loan `L001` with `ACTIVE`, `OVERDUE`, and `PAID` all on `2026-09-12`), deterministic business precedence applies:
     **PAID** (terminal) > **OVERDUE** > **ACTIVE**
4. **Payment State Lifecycle & Reversal Reconciliation:**
   * Payment deduplication evaluates the latest status per `payment_id`.
   * Only payments where `payment_status = 'SUCCESS'` and `payment_date <= report_date` contribute to `total_paid`.
   * If a transaction is initially `SUCCESS` and subsequently updated to `REVERSED` on or before `report_date`, the latest `REVERSED` status completely nullifies the transaction (evaluated at `payment_id` level before aggregation).
5. **100% Principal Payment Allocation:**
   * Per the spec: `outstanding_principal = principal_amount - total_paid`. All successful payments are allocated directly against the principal.
6. **Partition Pruning & Cost Control:**
   * Every query strictly enforces `ingestion_date <= :report_date`, pruning historical partitions and eliminating unnecessary full-table scans across 5+ TB of data.

---

## 3. End-to-End System Architecture

The proposed data platform uses a **Medallion Architecture (Bronze -> Silver -> Gold -> Regulatory Views)** orchestrated with Airflow and dbt.

![Data Solution Design Architecture](Diagram%201.png)

### Architectural Flow:
1. **Ingestion (Core Banking to Bronze):**
   Transactional data from core banking databases is captured via GCP Datastream (serverless CDC append-only) and ingested directly into BigQuery `raw` tables partitioned by `ingestion_date`.
2. **Deduplication & Curation (Bronze to Silver):**
   dbt models prune raw partitions (`ingestion_date <= report_date`) and deduplicate records using `ROW_NUMBER() = 1` with business tie-breakers (`PAID > OVERDUE > ACTIVE`). Processed incrementally into the `silver` dataset.
3. **Canonical Join (Silver to Gold):**
   The heavy join between loans, users, and payment aggregates is computed once in `fact_loan_daily` (materialized incrementally, clustered by `[loan_id, user_id]`). This serves as the single source of truth across all reporting.
4. **Serving Layer (Gold to Regulatory Views):**
   Output A (`daily_loan_performance`), Output B (`daily_user_loan_summary`), and other regulatory reports read from the pre-aggregated Gold mart as lightweight SQL views.
5. **Observability, Lineage & Alerting:**
   Airflow DAG SLAs and BigQuery slot usage are tracked in Grafana, column-level data lineage is cataloged via OpenMetadata, and financial reconciliation alerts trigger immediate Slack notifications.

---

## 4. Technology Stack & Tool Rationale

| Tool | Role in Architecture | Technical Rationale & Benefit |
| :--- | :--- | :--- |
| **Google BigQuery** | Cloud Data Warehouse | Serverless, highly scalable engine supporting petabyte-scale queries. Native partition pruning on `ingestion_date` and clustering on `[loan_id, user_id]` minimizes scanning costs. |
| **dbt Core** | Transformation & Modeling | Implements modular SQL models (Bronze -> Silver -> Gold -> Regulatory), Jinja parameterization, and data quality assertions. |
| **Apache Airflow** | Enterprise Orchestration | Manages task dependencies, execution date (`{{ ds }}`) injection, dynamic DAG generation from YAML, retries, and automated SLA alerts. |
| **GCP Datastream** | Real-Time CDC Ingestion | Serverless change data capture ingesting transaction logs from core banking databases directly into BigQuery Bronze tables with minimal latency. |
| **GCP Secret Manager** | Security & Secret Vault | Centralized credential management. Eliminates hardcoded tokens for regulatory API portals and SFTP endpoints. |
| **Grafana** | Pipeline Observability | Real-time dashboards monitoring Airflow DAG run durations, BigQuery slot consumption, row ingestion volumes, and SLA tracking. |
| **OpenMetadata** | Data Governance & Lineage | Automatic data cataloging, column-level data lineage tracking from Raw to Regulatory Views, and data contract enforcement. |
| **Slack** | Incident Alerting | Automated SEV-1 failure alerts for Airflow task timeouts and dbt reconciliation quality gate breaches. |

---

## 5. Part a: Reporting System Design & Queries

Suppose the reporting system is already built up and only requires queries to pull the data. Below are the engineering strategies and exact dbt SQL queries designed to fulfill the regulatory requirements.

### Strategy Addressing the 4 Core Problems:
1. **Duplicate Records:** Resolved in the Silver layer using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_date DESC)` plus business tie-breaker ordering (`PAID > OVERDUE > ACTIVE`).
2. **Slow Performance:** Resolved via partition pruning (`ingestion_date <= :report_date`) and performing the 3-way join once in `gold.fact_loan_daily`. Downstream regulatory reports read from the pre-aggregated Gold mart in under 1 second.
3. **Inconsistent Payment States:** Deduplicated at transaction level; filtered strictly by `payment_status = 'SUCCESS'` and `payment_date <= :report_date`.
4. **Manual Submission:** Replaced with an automated scheduled Airflow DAG with an automated financial reconciliation audit gate.

---

### Step 1: Silver Layer Models (Deduplication & Curation)

#### `models/silver/loan_account.sql`
Extracts the latest loan state on or before `report_date`, applying the deterministic tie-breaker rule:
```sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'loan_id',
    cluster_by = ["loan_id", "user_id"],
    schema = 'silver'
  )
}}

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
```

#### `models/silver/dim_users.sql`
Resolves the latest SCD Type 1 demographic profile per `user_id`:
```sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'user_id',
    cluster_by = ["user_id"],
    schema = 'silver'
  )
}}

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
```

#### `models/silver/loan_payment.sql`
Resolves the latest lifecycle version per `payment_id`:
```sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'payment_id',
    cluster_by = ["loan_id"],
    schema = 'silver'
  )
}}

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
```

---

### Step 2: Gold Layer Canonical Mart (Single Heavy Join)

#### `models/gold/fact_loan_daily.sql`
Combines loans, user demographics, and aggregated successful payments into a single canonical wide fact table. Materialized incrementally and clustered by `[loan_id, user_id]`:
```sql
{{
  config(
    materialized = 'incremental',
    unique_key = 'loan_id',
    cluster_by = ["loan_id", "user_id"],
    schema = 'gold'
  )
}}

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
```

---

### Step 3: Regulatory Serving Layer (Output Views)

#### Regulatory Output A: Daily Loan Performance
* **Grain:** `report_date × loan_id`
* **File:** `models/regulatory/daily_loan_performance.sql`
```sql
{{
  config(
    materialized = 'view',
    schema = 'regulatory'
  )
}}

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
```

#### Regulatory Output B: Daily User Loan Summary
* **Grain:** `report_date × user_id`
* **File:** `models/regulatory/daily_user_loan_summary.sql`
```sql
{{
  config(
    materialized = 'view',
    schema = 'regulatory'
  )
}}

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
```

---

### Step 4: Automated Financial Quality Gate Audit

To guarantee zero discrepancy between granular loan records and aggregated user summaries prior to regulatory submission, a dbt singular assertion test is executed:

#### `tests/assert_reconciliation_balance.sql`
```sql
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
```
* **Pipeline Rule:** Returns 0 rows on success. If any variance is detected, Airflow halts the pipeline and alerts the compliance team.

---

## 6. Part b: Handling 10+ Other Unique Regulatory Pipelines

### The Core Architectural Problem:
If 10 other unique regulatory pipelines were handled using the traditional approach of querying the raw schema directly:
* **High Compute Costs:** 10 separate pipelines each scanning 2 TB (loans) + 3 TB (payments) + 50 GB (users) = 50+ TB scanned every day.
* **Failure Risk & Lock Contention:** Multiple pipelines simultaneously running full-table scans and window functions on raw CDC tables.
* **Code Duplication:** Business logic for deduplication and payment reversals repeated across 10+ different SQL scripts.

---

### Assumptions for Part b:
1. **Independent Schedules & SLAs:** Different regulators require data on different cadences (daily, weekly, monthly) and strict submission cutoffs (e.g. 06:00 vs 09:00 WIB). Pipelines must run independently without blocking one another.
2. **Shared Canonical Domain Model:** Regulatory reports across authorities primarily derive from the core loan lifecycle, user demographics, and payment transactions. Over 90% of regulatory queries can be fulfilled directly from a single pre-joined Gold Mart.
3. **Decoupled Failure Blast Radius:** A schema failure, API rejection, or portal downtime at one regulatory agency must never delay submissions to other agencies.

---

### Tools Used for Part b:
* **Apache Airflow (Cloud Composer):** Dynamic DAG factory pattern reading declarative YAML files to instantiate independent DAGs per pipeline without Python boilerplate.
* **dbt Core:** Materializes lightweight SQL views from the Gold Mart, keeping transformations modular and version-controlled.
* **Google BigQuery:** Leverages partition pruning on `report_date` and clustering on `[loan_id, user_id]` to serve all downstream regulatory views with sub-second response times.
* **YAML Declarative Engine:** Decouples pipeline configuration (schedule, SLAs, models) from pipeline orchestration logic.

---

### Proposed Strategy: Config-Driven Dynamic Pipeline Framework

The design separates data computation from pipeline orchestration:
* **Canonical Mart (`gold.fact_loan_daily`):** Performs the heavy 3-way join once per day.
* **10+ Regulatory Views:** Lightweight SQL views filter and project directly from the Gold mart.
* **Declarative Config (`regulatory_pipelines.yaml`):** Each pipeline is declared with its schedule, destination, and target query.
* **Dynamic DAG Factory (`dynamic_regulatory_dag_factory.py`):** Automatically instantiates Airflow DAGs from YAML configs without writing duplicate Python code.

#### 1. The Canonical Mart Strategy ("Compute Once, Serve Many")
* All 10+ regulatory pipelines read exclusively from `gold.fact_loan_daily`.
* Because `fact_loan_daily` is already clean, deduplicated, and clustered by `[loan_id, user_id]`, downstream regulatory queries execute as lightweight views in under 1 second.
* **Result:** Daily BigQuery scan volume drops by over 90%.

#### 2. Declarative Configuration via YAML (`regulatory_pipelines.yaml`)
To onboard a new regulatory pipeline, a Data Engineer **does not need to write new Airflow Python DAG code**. They simply:
1. Create a lightweight dbt view model filtering/projecting from `fact_loan_daily`.
2. Add a declarative 6-line entry in `dags/configs/regulatory_pipelines.yaml`:

```yaml
regulatory_pipelines:
  - id: daily_loan_performance
    name: "Regulatory Output A - Daily Loan Performance"
    dbt_model: "daily_loan_performance"
    task_script: "regulatory_output_daily_loan.py"
    schedule: "0 6 * * *"
    sla_time: "06:00"
    active: true

  - id: daily_user_loan_summary
    name: "Regulatory Output B - Daily User Loan Summary"
    dbt_model: "daily_user_loan_summary"
    task_script: "regulatory_output_daily_user_loan_summary.py"
    schedule: "0 6 * * *"
    sla_time: "06:00"
    active: true

  - id: other_regulatory
    name: "Regulatory Output C (Sample) - SLIK OJK NPL Monitoring"
    dbt_model: "other_regulatory"
    task_script: "regulatory_output_other_regulatory.py"
    schedule: "0 9 * * *"
    sla_time: "09:00"
    active: true

  - id: other_sample_regulatory
    name: "Regulatory Output D (Sample) - Bank Indonesia Macro"
    dbt_model: "daily_loan_performance"
    schedule: "0 7 * * 1"
    sla_time: "08:00"
    active: true
```

#### 3. Airflow Dynamic DAG Factory (`dynamic_regulatory_dag_factory.py`)
Our dynamic factory automatically instantiates independent Airflow DAGs for each configured pipeline:
* **Execution Options:** If `task_script` is omitted, the factory runs the dbt model directly; otherwise it executes the dedicated custom task handler.
* **Independent Scheduling:** Each regulatory authority can have a different submission schedule (e.g. daily, weekly, monthly).
* **Fault Isolation:** A failure in one regulatory portal does not block or fail other pipelines.

#### 4. Attached Query Sample: 3rd Regulatory Pipeline (`other_regulatory.sql`)
Demonstrating how effortless it is to create a sample Non-Performing Loan (NPL) exposure view directly from Gold:
```sql
{{ config(materialized = 'view', schema = 'regulatory') }}

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
```

---

## 7. Architectural Comparison: Traditional vs. Monee Proposed

| Evaluation Metric | Traditional Approach (10 Raw Pipelines) | Proposed Medallion + Config Factory |
| :--- | :--- | :--- |
| **Daily BigQuery Scan Volume** | 10 x 5.05 TB = **50.5 TB/day** | 1 x 5.05 TB + negligible view scans = **~5.1 TB/day** (90% savings) |
| **Join Computation Overhead** | 10 separate 3-way multi-terabyte joins daily | **1x canonical join** executed at Gold mart |
| **New Pipeline Onboarding Time** | 2-4 days (writing new queries, joins, & DAGs) | **< 30 minutes** (1 SQL view + YAML entry) |
| **Data Consistency Risk** | High (inconsistent deduplication/tie-breaker logic) | **Zero** (All downstream reports share identical Gold SSOT) |
| **Failure Blast Radius** | High (one long-running query locks resources) | **Isolated** (each DAG runs on independent schedule & SLA) |
| **Data Quality Assurance** | Manual checks or post-submission audits | **Pre-submission automated reconciliation gate** |

---

## 8. Summary of Deliverables in Workspace

All models, orchestrator scripts, and test gates referenced in this document are fully implemented and testable in this workspace:
* **Architecture Diagram:** [Diagram 1.png](Diagram%201.png)
* **Central GCP Configuration & Secret Vault:** [GCP_Config.py](GCP_Config.py)
* **dbt Project & Models:**
  * Project config: [dbt_project.yml](Query%20(dbt%20model)/dbt_project.yml)
  * Silver models: [loan_account.sql](Query%20(dbt%20model)/models/silver/loan_account.sql), [dim_users.sql](Query%20(dbt%20model)/models/silver/dim_users.sql), [loan_payment.sql](Query%20(dbt%20model)/models/silver/loan_payment.sql)
  * Gold Core Mart: [fact_loan_daily.sql](Query%20(dbt%20model)/models/gold/fact_loan_daily.sql)
  * Regulatory Views: [daily_loan_performance.sql](Query%20(dbt%20model)/models/regulatory/daily_loan_performance.sql), [daily_user_loan_summary.sql](Query%20(dbt%20model)/models/regulatory/daily_user_loan_summary.sql), [other_regulatory.sql](Query%20(dbt%20model)/models/regulatory/other_regulatory.sql) (Sample Output C)
  * Financial Quality Gate Test: [assert_reconciliation_balance.sql](Query%20(dbt%20model)/tests/assert_reconciliation_balance.sql)
* **Airflow Orchestration:**
  * Declarative Config: [regulatory_pipelines.yaml](dags/configs/regulatory_pipelines.yaml)
  * Dynamic DAG Factory: [dynamic_regulatory_dag_factory.py](dags/dynamic_regulatory_dag_factory.py)
  * Master Scheduled Pipeline: [main_scheduled_pipeline.py](dags/main_scheduled_pipeline.py)

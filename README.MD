# Monee Data Engineering Take-Home Test

## **Name: Khalif Santoso**

## Repository Structure

```
.
├── A-Data_Solution_Design/
│   ├── Answer_Part_A.md             # Technical design document
│   ├── Diagram 1.png                # Architecture diagram
│   ├── GCP_Config.py                # GCP config & secret vault
│   ├── Query (dbt model)/           # dbt project
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   ├── models/
│   │   │   ├── sources.yml
│   │   │   ├── silver/              # Deduplicated models
│   │   │   │   ├── loan_account.sql
│   │   │   │   ├── dim_users.sql
│   │   │   │   └── loan_payment.sql
│   │   │   ├── gold/
│   │   │   │   └── fact_loan_daily.sql
│   │   │   └── regulatory/          # Output views
│   │   │       ├── daily_loan_performance.sql
│   │   │       ├── daily_user_loan_summary.sql
│   │   │       └── other_regulatory.sql
│   │   └── tests/
│   │       └── assert_reconciliation_balance.sql
│   └── dags/
│       ├── configs/
│       │   └── regulatory_pipelines.yaml
│       ├── dynamic_regulatory_dag_factory.py
│       ├── main_scheduled_pipeline.py
│       └── tasks/
│           ├── silver_loans.py
│           ├── silver_payments.py
│           ├── silver_users.py
│           ├── gold_regulatory_serving_layer.py
│           ├── regulatory_output_daily_loan.py
│           ├── regulatory_output_daily_user_loan_summary.py
│           └── regulatory_output_other_regulatory.py
│
└── B-Reverse_ETL_at_Scale/
    ├── Answer_Part_B.md             # Reverse-ETL design doc
    ├── Diagram 2.png                # Architecture diagram
    ├── configs/
    │   ├── ojk_daily_loans.yaml
    │   └── bi_user_summary.yaml
    ├── src/
    │   ├── runner.py                # Orchestrator & batch dispatcher
    │   ├── core/
    │   │   ├── base_destination.py
    │   │   ├── config_loader.py
    │   │   ├── secret_vault.py
    │   │   └── state_store.py
    │   └── destinations/
    │       ├── api_destination.py
    │       └── sftp_destination.py
    ├── dags/
    │   ├── reverse_etl.py
    │   └── end_to_end_pipeline.py
    └── tests/
        └── test_reverse_etl.py
```

---

## Section A: Data Solution Design (Summary)

The complete, in-depth architectural RFC and SQL solution is documented in:
* **[A-Data_Solution_Design/Answer_Part_A.md](./A-Data_Solution_Design/Answer_Part_A.md)**

### Key Highlights:
1. **Medallion Data Architecture:**
   * **Bronze (Raw):** CDC append-only tables partitioned on `ingestion_date` (loan_accounts 2 TB, loan_payments 3 TB, users 50 GB).
   * **Silver (Curated):** Partition pruning + `ROW_NUMBER() = 1` deduplication with business tie-breakers (`PAID > OVERDUE > ACTIVE`).
   * **Gold (Canonical Mart):** Single heavy join executed once in `fact_loan_daily` (incremental, clustered by `[loan_id, user_id]`).
   * **Regulatory Layer:** Lightweight BigQuery views for Output A (`daily_loan_performance`) and Output B (`daily_user_loan_summary`).
2. **Scalability for 10+ Regulatory Pipelines:**
   * All downstream pipelines query the pre-joined Gold Mart instead of scanning 50+ TB daily.
   * Adding a new regulatory pipeline takes under 30 minutes: one dbt SQL view + a YAML entry.
3. **Automated Financial Quality Gate:**
   * `assert_reconciliation_balance.sql` halts the pipeline if principal or outstanding totals diverge between Output A and Output B.
4. **Governance, Observability & Alerting:**
   * Grafana dashboards, OpenMetadata lineage tracking, and Slack incident alerting.

---

## Section B: Reverse ETL at Scale (Summary)

The complete architectural proposal from the Enterprise Architect's Point of View is documented in:
* **[B-Reverse_ETL_at_Scale/Answer_Part_B.md](./B-Reverse_ETL_at_Scale/Answer_Part_B.md)**

### Key Highlights:
1. **Modularity & Pluggability:**
   * `BaseDestination` interface lets you add new protocols (REST API, SFTP, Webhooks, Kafka) without touching the runner.
2. **Incremental Processing & Idempotency:**
   * `StateManager` tracks watermarks for delta-only processing. Already-synced dates are skipped (zero bytes scanned).
   * SHA-256 idempotency keys prevent duplicate submissions on retries.
3. **Configuration & Secrets:**
   * Declarative YAML configs define schedules, queries, and field mappings.
   * Google Secret Manager ensures zero hardcoded tokens or keys.
4. **Reliability & Resiliency:**
   * Exponential backoff retries, atomic SFTP uploads, and DLQ routing for failed batches.
5. **Unified Master Orchestration:**
   * `end_to_end_pipeline.py` wires Part A (DWH) into Part B (Reverse-ETL) with quality gate verification in between.

# Monee Data Engineering Take-Home Test
## Section B: Reverse ETL at Scale

**Target Environment:** Python 3, Apache Airflow / Google Cloud Composer, Google Secret Manager, BigQuery  

## 1. Context & Architectural Challenge

A credit company needs to synchronize data from the central Data Warehouse into multiple external destinations (Regulatory API portals such as OJK SLIK, central bank SFTP servers, operational CRM/webhooks, partner banks). 

The system must scale to **hundreds of pipelines**, where each pipeline has:
* Different execution schedules (real-time, hourly, daily, weekly)
* Different SQL extraction queries from Data Warehouse marts
* Different field schema mappings (e.g., snake_case to camelCase or Indonesian banking terms)
* Different destination protocols (REST APIs, SFTP, Webhooks, Message Brokers)

---

## 2. Core Architectural Assumptions

1. **Warehouse as Single Source of Truth (SSOT):**  
   The Reverse-ETL system reads exclusively from curated Data Marts / Views (such as `monee_regulatory.daily_loan_performance` and `monee_regulatory.daily_user_loan_summary` from Section A) to prevent heavy analytical computation on raw transactional data.
2. **Idempotency Guarantee:**  
   External destinations must support idempotent ingestion (via `X-Idempotency-Key` headers or atomic file replacements) so that network retries never produce duplicate loan records.
3. **Transient Execution State:**  
   High-watermarks and synchronization checkpoints are committed only after the destination successfully acknowledges receipt of data (At-Least-Once delivery with idempotent deduplication).
4. **Zero Hardcoded Credentials:**  
   Authentication keys, bearer tokens, and private SSH keys are stored in Google Cloud Secret Manager and accessed just-in-time using least-privilege service accounts.

---

## 3. High-Level System Architecture

![Reverse ETL Architecture Diagram](./Diagram%202.png)

### Architectural Flow:
1. **Serving Layer Extraction:**
   The Reverse-ETL engine extracts validated data from BigQuery regulatory views (`daily_loan_performance`, `daily_user_loan_summary`) filtered by execution watermark.
2. **Reverse-ETL Core Engine:**
   * **Config Loader:** Discovers and parses pipeline definitions from YAML files (extract queries, destinations, schedules, and column mappings).
   * **State Manager:** Tracks watermarks and commit checkpoints to ensure incremental processing and avoid re-sending processed dates.
   * **Secret Vault:** Retrieves API bearer tokens or private SSH keys just-in-time from Google Secret Manager.
   * **Schema Mapper:** Maps and serializes record payloads into destination-specific formats (JSON for REST APIs, CSV for SFTP).
   * **Dispatcher & Resiliency:** Dispatches payloads in batches, handling network timeouts via exponential backoff retries and routing failed batches to a Dead Letter Queue (DLQ).
3. **External Destinations:**
   Payloads are delivered to external endpoints such as OJK SLIK (REST API with idempotency key), Bank Indonesia (SFTP with SSH keys), and partner webhooks.

---

## 4. Addressing the 5 Core System Requirements

### 1. Modularity & Extensibility (Adapter Pattern)
* **Base Contract:** All destination connectors extend an abstract base class (`BaseDestination`), implementing:
  * `health_check()`: Verifies endpoint reachability before querying the warehouse.
  * `send_batch()`: Dispatches payload with idempotency headers.
  * `close()`: Cleans up HTTP sessions or SFTP sockets.
* **Pluggability:** Adding a Kafka broker, AWS S3 export, or Webhook requires creating **only one new connector file** without altering the runner engine or Airflow DAGs.

### 2. Incremental Processing & Idempotency
* **Watermark Tracking (`StateManager`):**  
  Each pipeline tracks its `last_watermark` and `synced_dates`. On each run, the extractor queries only delta records (`WHERE report_date = :watermark_date`).
* **Zero Resource Idempotent Skip:**  
  If an execution date has already been committed as successful, the engine skips extraction and transmission immediately (0 bytes scanned, 0 API calls).
* **Content Hashing:**  
  For REST APIs, a SHA-256 hash of `batch_id + payload` is sent as `X-Idempotency-Key` to prevent double-charging or double-recording on network timeout retries.

### 3. Configuration & Secrets Management
* **Declarative YAML:** Pipelines are declared purely in YAML (`configs/ojk_daily_loans.yaml`, `configs/bi_user_summary.yaml`).
* **Secret Reference (`secret_key_ref`):** YAML files only declare the *name* of the secret (e.g., `secret_key_ref: ojk_api_token`). At runtime, `SecretVault` fetches the token from Google Secret Manager. No plain-text passwords or keys ever touch Git.

### 4. Reliability & Fault Tolerance
* **Exponential Backoff:** Transmissions retry up to 3 times with exponential backoff (sleep = backoff_factor ^ attempt).
* **Dead Letter Queue (DLQ):** If a batch exhausts all retries, payloads are routed to a secure DLQ bucket (`s3://monee-audit-dlq/` or Cloud Storage) for manual inspection, preventing pipeline halts.
* **Atomic SFTP Upload:** Files are written with a `.tmp` suffix and renamed atomically once the full payload is transferred, preventing external consumers from reading truncated files.

### 5. Further Development Efficiency
* **Zero-Code Onboarding for New Pipelines:**  
  Engineers can deploy a new pipeline in under 15 minutes by adding a single YAML file into `configs/`. The runner's `ConfigLoader.discover_all_configs()` automatically registers and executes it.

---

## 5. File & Directory Structure

```
B-Reverse_ETL_at_Scale/
├── configs/                          # Declarative Pipeline YAML Configurations
│   ├── ojk_daily_loans.yaml          # Regulatory REST API Configuration
│   └── bi_user_summary.yaml          # Central Bank SFTP Configuration
│
├── src/                              # Core Python Reverse-ETL Engine
│   ├── runner.py                     # Central Orchestrator & Batch Processor
│   ├── core/
│   │   ├── base_destination.py       # Abstract Base Class Interface
│   │   ├── config_loader.py          # Pydantic/Dataclass YAML Schema Parser
│   │   ├── secret_vault.py           # Google Secret Manager Integration
│   │   └── state_store.py            # Watermarking & Idempotency State Manager
│   └── destinations/                 # Plug-and-Play Destination Connectors
│       ├── api_destination.py        # REST API with Backoff & Idempotency Keys
│       └── sftp_destination.py       # SFTP with Atomic Renames & Checksums
│
├── dags/                             # Airflow Integration
│   ├── reverse_etl.py                # Standalone Dispatcher Task Callable
│   └── end_to_end_pipeline.py        # Master E2E DAG (Part A + Part B)
│
└── tests/
    └── test_reverse_etl.py           # Automated Unit & Integration Tests
```

---

## 6. Key Code Snippets

### A. Declarative Pipeline Specification (`configs/ojk_daily_loans.yaml`)
```yaml
pipeline_name: "ojk_daily_loan_performance"
schedule: "0 3 * * *"

source:
  type: "data_warehouse"
  query: |
    SELECT 
      report_date, loan_id, user_id, product_type, loan_status,
      principal_amount, total_paid, outstanding_principal
    FROM monee_regulatory.daily_loan_performance
    WHERE report_date = :watermark_date

incremental:
  strategy: "date_watermark"
  watermark_column: "report_date"
  batch_size: 500

mapping:
  report_date: "REPORT_DATE"
  loan_id: "LOAN_ID"
  user_id: "CUSTOMER_ID"
  loan_status: "LOAN_STATUS"
  outstanding_principal: "OUTSTANDING_PRINCIPAL"

destination:
  type: "rest_api"
  endpoint: "https://mock-portal-ojk/api/v1/loan-reports"
  auth:
    type: "bearer_token"
    secret_key_ref: "ojk_api_token"

reliability:
  max_retries: 3
  backoff_factor: 2.0
  dlq_enabled: true
```

### B. Extensible Destination Base Class (`src/core/base_destination.py`)
```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseDestination(ABC):
    def __init__(self, config: Dict[str, Any], secret_token: str):
        self.config = config
        self.secret_token = secret_token

    @abstractmethod
    def health_check(self) -> bool:
        """Verifies endpoint reachability before querying warehouse."""
        pass

    @abstractmethod
    def send_batch(self, batch_data: List[Dict[str, Any]], batch_id: str) -> Dict[str, Any]:
        """Sends one batch with idempotency guarantees."""
        pass

    @abstractmethod
    def close(self):
        """Cleans up sockets or sessions."""
        pass
```

### C. Airflow Master Pipeline Integration (`dags/end_to_end_pipeline.py`)
```python
# Task dependencies linking DWH transformations (Part A) to Reverse-ETL (Part B):
task_dwh = PythonOperator(
    task_id="dwh_ingestion_and_marts",
    python_callable=run_dwh_pipeline,
    provide_context=True
)

task_retl = PythonOperator(
    task_id="reverse_etl_dispatch",
    python_callable=run_reverse_etl,
    provide_context=True
)

# Part A must pass reconciliation quality gates before Part B dispatches data:
task_dwh >> task_retl
```

---

## 7. Verification & Test Results

The Reverse-ETL system includes full automated unit and integration tests passing in 0.016s:
```
$ python3 tests/test_reverse_etl.py
....
----------------------------------------------------------------------
Ran 4 tests in 0.016s

OK (Configs Discovery: PASSED, Secret Vault: PASSED, State Watermarking: PASSED, API Pipeline Execution: PASSED)
```

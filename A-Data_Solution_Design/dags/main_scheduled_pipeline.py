"""
DWH scheduled pipeline orchestrator.
Executes Silver deduplication, canonical Gold mart consolidation,
dynamic regulatory views, and financial reconciliation quality gates.
"""

import os
import sys
import time
from datetime import datetime, timedelta

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(DAGS_DIR)

for p in [PROJECT_DIR, DAGS_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from GCP_Config import get_report_date, GCP_CONFIG
from dags.tasks.silver_loans import run_silver_loans
from dags.tasks.silver_payments import run_silver_payments
from dags.tasks.silver_users import run_silver_users
from dags.tasks.gold_regulatory_serving_layer import run_gold_fact
from dags.tasks.regulatory_output_daily_user_loan_summary import run_reconciliation_gate
from dags.dynamic_regulatory_dag_factory import load_regulatory_yaml, get_regulatory_callable

def run_dwh_pipeline(context=None, override_date=None):
    start_time = time.time()
    report_date = get_report_date(context, override_date)
    reg_configs = load_regulatory_yaml()
    active_regulations = [r for r in reg_configs if r.get("active", True)]
    
    print("\n" + "=" * 60)
    print(f"Starting DWH pipeline execution for date: {report_date}")
    print(f"GCP Project: {GCP_CONFIG['project_id']} ({GCP_CONFIG['region']})")
    print(f"Active regulations: {len(active_regulations)} discovered from YAML")
    print("=" * 60)
    
    # 1. Silver deduplication
    print("\n--- Phase 1: Silver Deduplication ---")
    print("Running loan_account...")
    r1 = run_silver_loans(context, report_date)
    print("Running loan_payment...")
    r2 = run_silver_payments(context, report_date)
    print("Running dim_users...")
    r3 = run_silver_users(context, report_date)
    
    # 2. Canonical Gold Mart (single heavy join)
    print("\n--- Phase 2: Gold Fact Mart ---")
    print("Running fact_loan_daily...")
    r4 = run_gold_fact(context, report_date)
    
    # 3. Dynamic Regulatory Views
    print("\n--- Phase 3: Regulatory Output Views ---")
    reg_results = []
    for reg in active_regulations:
        print(f"Running {reg['id']}...")
        fn = get_regulatory_callable(reg)
        res = fn(context, report_date)
        reg_results.append(res)
        
    # 4. Quality Gate
    print("\n--- Phase 4: Financial Reconciliation Gate ---")
    r_test = run_reconciliation_gate(context, report_date)
    
    elapsed = round(time.time() - start_time, 2)
    print("=" * 60)
    print(f"Pipeline completed successfully in {elapsed}s")
    print(f"Gold Mart   : {GCP_CONFIG['project_id']}.{GCP_CONFIG['gold_dataset']}")
    print(f"Regulatory  : {GCP_CONFIG['project_id']}.{GCP_CONFIG['regulatory_dataset']}")
    print("=" * 60 + "\n")
    
    return {
        "status": "SUCCESS",
        "dag_id": "main_scheduled_pipeline",
        "report_date": report_date,
        "elapsed_seconds": elapsed,
        "active_regulations": [r["id"] for r in active_regulations]
    }

# --------------------------------------------------------------------------
# 2. AIRFLOW DAG DEFINITION (Dynamic Task Generation for Cloud Composer)
# --------------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

default_args = {
    "owner": "monee_data_platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="main_scheduled_pipeline",
        default_args=default_args,
        description="Airflow DWH Pipeline: Silver -> Gold Fact -> Dynamic Regulatory Tasks (YAML-Driven)",
        schedule_interval="0 1 * * *",  # Daily 01:00 AM UTC
        catchup=False,
        max_active_runs=1,
        tags=["dwh", "scheduled", "monee", "config_driven", "gcp", "bigquery"]
    )

    with dag:
        task_silver_loans = PythonOperator(
            task_id="silver_loans",
            python_callable=run_silver_loans,
            provide_context=True
        )

        task_silver_payments = PythonOperator(
            task_id="silver_payments",
            python_callable=run_silver_payments,
            provide_context=True
        )

        task_silver_users = PythonOperator(
            task_id="silver_users",
            python_callable=run_silver_users,
            provide_context=True
        )

        task_gold_fact = PythonOperator(
            task_id="gold_fact_daily",
            python_callable=run_gold_fact,
            provide_context=True
        )

        task_quality_gate = PythonOperator(
            task_id="reconciliation_quality_gate",
            python_callable=run_reconciliation_gate,
            provide_context=True
        )

        # Dependency: Silver (Parallel) -> Gold Fact
        [task_silver_loans, task_silver_payments, task_silver_users] >> task_gold_fact

        # DYNAMIC TASK GENERATION:
        # Dynamically loop through all active regulations defined in regulatory_pipelines.yaml
        regulatory_configs = load_regulatory_yaml()
        regulatory_tasks = []

        for reg in regulatory_configs:
            if reg.get("active", True):
                fn = get_regulatory_callable(reg)
                t_reg = PythonOperator(
                    task_id=f"regulatory_{reg['id']}",
                    python_callable=fn,
                    provide_context=True
                )
                # Wire downstream of Gold Fact, and upstream of Quality Gate Audit
                task_gold_fact >> t_reg >> task_quality_gate
                regulatory_tasks.append(t_reg)

# --------------------------------------------------------------------------
# 3. DIRECT TERMINAL RUNNER (python3 main_scheduled_pipeline.py 2026-09-12)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_dwh_pipeline(override_date=date_arg)

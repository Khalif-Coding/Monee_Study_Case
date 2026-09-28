"""
Master pipeline combining DWH ingestion (Part A) and Reverse-ETL dispatch (Part B).
Can be triggered directly from CLI or scheduled via Airflow.
"""

import os
import sys
import time
from datetime import datetime, timedelta

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PART_B_DIR = os.path.dirname(DAGS_DIR)
WORKSPACE_ROOT = os.path.dirname(PART_B_DIR)
PART_A_DIR = os.path.join(WORKSPACE_ROOT, "A-Data_Solution_Design")

for p in [WORKSPACE_ROOT, PART_A_DIR, PART_B_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from GCP_Config import get_report_date, run_step_0_runner_info, GCP_CONFIG
from dags.main_scheduled_pipeline import run_dwh_pipeline
from dags.reverse_etl import run_reverse_etl

def run_master_pipeline(context=None, override_date=None):
    master_start = time.time()
    report_date = get_report_date(context, override_date)
    
    print("\n" + "=" * 60)
    print(f"Starting master pipeline for date: {report_date}")
    print(f"Platform: GCP ({GCP_CONFIG['project_id']}) | Region: {GCP_CONFIG['region']}")
    print("=" * 60)
    
    # 1. Environment & credentials
    run_step_0_runner_info()
    
    # 2. Stage 1: DWH Ingestion & Transformations (Part A)
    print("\nStage 1: Running DWH Ingestion and Marts...")
    dwh_result = run_dwh_pipeline(context, report_date)
    
    # 3. Stage 2: Reverse-ETL Dispatcher (Part B)
    print("\nStage 2: Running Reverse-ETL Dispatcher...")
    retl_result = run_reverse_etl(context, report_date)
    
    total_elapsed = round(time.time() - master_start, 2)
    
    print("\n" + "=" * 60)
    print(f"Master pipeline completed in {total_elapsed}s for date {report_date}")
    print("Stage 1 - DWH Ingestion & Quality Gate : SUCCESS")
    print("Stage 2 - Reverse-ETL Dispatch          : SUCCESS")
    print("=" * 60 + "\n")
    
    return {
        "status": "SUCCESS",
        "dag_id": "master_end_to_end_pipeline",
        "report_date": report_date,
        "total_elapsed_seconds": total_elapsed,
        "dwh_result": dwh_result,
        "reverse_etl_result": retl_result
    }

# Airflow DAG definition
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
        dag_id="master_end_to_end_pipeline",
        default_args=default_args,
        description="Airflow Master End-to-End Orchestrator: Part A (DWH) -> Part B (Reverse-ETL)",
        schedule_interval="0 1 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["master", "end_to_end", "dwh", "reverse_etl", "gcp"]
    )

    with dag:
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

        task_dwh >> task_retl

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_master_pipeline(override_date=date_arg)

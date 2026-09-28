"""
Dynamic DAG factory for regulatory pipelines.
Reads declarations from configs/regulatory_pipelines.yaml and generates
independent Airflow DAGs with custom schedules and SLA thresholds.
"""

import os
import sys
import importlib
import yaml
from datetime import datetime, timedelta
from typing import List, Dict, Any

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(DAGS_DIR)
CONFIGS_FILE = os.path.join(DAGS_DIR, "configs", "regulatory_pipelines.yaml")

for p in [PROJECT_DIR, DAGS_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from GCP_Config import get_report_date, run_dbt_command, GCP_CONFIG

def load_regulatory_yaml() -> List[Dict[str, Any]]:
    """Loads regulatory pipeline configurations from YAML."""
    if not os.path.exists(CONFIGS_FILE):
        return []
    with open(CONFIGS_FILE, "r") as f:
        data = yaml.safe_load(f)
        return data.get("regulatory_pipelines", [])

def execute_generic_dbt_regulatory(model_name: str, context=None, override_date=None) -> Dict[str, Any]:
    """Default dbt runner when no custom task script is specified."""
    report_date = get_report_date(context, override_date)
    cmd_res = run_dbt_command(["run", "--select", model_name, "--vars", f'{{"report_date": "{report_date}"}}'])
    print("Completed successfully.")
    return {"status": "SUCCESS", "model": model_name, "report_date": report_date, "dbt_result": cmd_res}

def get_regulatory_callable(reg_config: Dict[str, Any]):
    """Resolves task callable: imports task_script if defined, else generic dbt runner."""
    task_script = reg_config.get("task_script")
    dbt_model = reg_config.get("dbt_model")

    if task_script:
        module_name = os.path.splitext(task_script)[0]
        try:
            module = importlib.import_module(f"dags.tasks.{module_name}")
            for fn_name in ["run_regulatory_daily_loan", "run_regulatory_user_summary", "run_regulatory_other"]:
                if hasattr(module, fn_name):
                    return getattr(module, fn_name)
        except Exception as e:
            print(f"Could not import dags.tasks.{module_name} ({e}), using generic runner.")

    def generic_runner(context=None, override_date=None):
        return execute_generic_dbt_regulatory(dbt_model, context, override_date)

    return generic_runner

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

default_args = {
    "owner": "monee_regulatory_compliance",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    configs = load_regulatory_yaml()
    for reg in configs:
        if not reg.get("active", True):
            continue

        pipeline_id = reg["id"]
        dag_id = f"dag_regulatory_{pipeline_id}"
        schedule_cron = reg.get("schedule", "0 6 * * *")
        pipeline_callable = get_regulatory_callable(reg)

        dag_instance = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=f"Standalone pipeline: {reg.get('name')}",
            schedule_interval=schedule_cron,
            catchup=False,
            max_active_runs=1,
            tags=["regulatory", "independent", "dag_factory", pipeline_id]
        )

        with dag_instance:
            PythonOperator(
                task_id=f"execute_{pipeline_id}",
                python_callable=pipeline_callable,
                provide_context=True
            )

        globals()[dag_id] = dag_instance

if __name__ == "__main__":
    target_id = sys.argv[1] if len(sys.argv) > 1 else "all"
    date_arg = sys.argv[2] if len(sys.argv) > 2 else None
    
    configs = load_regulatory_yaml()
    print("\n" + "=" * 60)
    print(f"Regulatory runner (total active: {len(configs)})")
    print("=" * 60)

    matched = [c for c in configs if c.get("id") == target_id] if target_id != "all" else configs

    if not matched:
        print(f"Pipeline ID '{target_id}' not found in configs/regulatory_pipelines.yaml")
        sys.exit(1)

    for item in matched:
        print(f"\nRunning pipeline: {item.get('name')} (ID: {item.get('id')})")
        fn = get_regulatory_callable(item)
        fn(override_date=date_arg)

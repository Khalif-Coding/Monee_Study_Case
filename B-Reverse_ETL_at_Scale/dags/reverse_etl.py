"""
Reverse-ETL dispatcher task.
Dispatches data from regulatory marts to external partner/regulator endpoints.
"""

import os
import sys
import time

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(DAGS_DIR)
WORKSPACE_ROOT = os.path.dirname(BASE_DIR)
PART_A_DIR = os.path.join(WORKSPACE_ROOT, "A-Data_Solution_Design")

for p in [WORKSPACE_ROOT, PART_A_DIR, BASE_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from GCP_Config import get_report_date, GCP_CONFIG
except ImportError:
    from datetime import datetime, timezone, timedelta
    def get_report_date(context=None, override_date=None):
        if override_date:
            return override_date
        if context and context.get("ds"):
            return context["ds"]
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")
    GCP_CONFIG = {"project_id": "monee-data-platform-prod", "gold_dataset": "monee_gold", "regulatory_dataset": "monee_regulatory"}

from src.core.config_loader import ConfigLoader
from src.runner import ReverseEtlRunner

def run_reverse_etl(context=None, override_date=None, target_config_path=None):
    start_time = time.time()
    report_date = get_report_date(context, override_date)
    
    print("\n" + "=" * 60)
    print(f"Starting reverse-ETL sync for date: {report_date}")
    print(f"Source DWH: {GCP_CONFIG['project_id']}.{GCP_CONFIG.get('regulatory_dataset', 'monee_regulatory')}")
    print("=" * 60)
    
    runner = ReverseEtlRunner()
    configs_dir = os.path.join(BASE_DIR, "configs")
    
    synced_results = []
    if target_config_path:
        cfg = ConfigLoader.load_from_file(target_config_path)
        res = runner.run_pipeline(cfg, report_date)
        synced_results.append(res)
    else:
        configs = ConfigLoader.discover_all_configs(configs_dir)
        print(f"Found {len(configs)} active pipeline(s) in {configs_dir}")
        for cfg in configs:
            print(f"- Running {cfg.pipeline_name} (dest: {cfg.destination_type})")
            res = runner.run_pipeline(cfg, report_date)
            synced_results.append(res)
            
    elapsed = round(time.time() - start_time, 2)
    print("=" * 60)
    print(f"Reverse-ETL sync completed in {elapsed}s")
    print("=" * 60 + "\n")
    return {
        "status": "SUCCESS",
        "task": "reverse_etl_dispatcher",
        "report_date": report_date,
        "elapsed_seconds": elapsed,
        "pipelines_synced": synced_results
    }

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_reverse_etl(override_date=date_arg)

"""
Regulatory Output A task (Daily Loan Performance).
Generates report_date x loan_id view from gold fact mart.
"""

import os
import sys

TASKS_DIR = os.path.dirname(os.path.abspath(__file__))
DAGS_DIR = os.path.dirname(TASKS_DIR)
PROJECT_DIR = os.path.dirname(DAGS_DIR)
for p in [PROJECT_DIR, DAGS_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from GCP_Config import get_report_date, run_dbt_command, GCP_CONFIG

def run_regulatory_daily_loan(context=None, override_date=None):
    report_date = get_report_date(context, override_date)
    cmd_res = run_dbt_command(["run", "--select", "daily_loan_performance", "--vars", f'{{"report_date": "{report_date}"}}'])
    print("Completed successfully.")
    return {"status": "SUCCESS", "task": "output_a", "report_date": report_date, "dbt_result": cmd_res}

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_regulatory_daily_loan(override_date=date_arg)

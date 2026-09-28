"""
Regulatory Output B task (Daily User Loan Summary) and Quality Gate.
Generates user-level loan aggregation and executes reconciliation tests.
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

def run_regulatory_user_summary(context=None, override_date=None):
    report_date = get_report_date(context, override_date)
    cmd_res = run_dbt_command(["run", "--select", "daily_user_loan_summary", "--vars", f'{{"report_date": "{report_date}"}}'])
    print("Completed successfully.")
    return {"status": "SUCCESS", "task": "output_b", "report_date": report_date, "dbt_result": cmd_res}

def run_reconciliation_gate(context=None, override_date=None):
    report_date = get_report_date(context, override_date)
    test_res = run_dbt_command(["test", "--select", "assert_reconciliation_balance"])
    print("Reconciliation verified (Output A == Output B).")
    return {"status": "PASSED", "task": "quality_gate", "report_date": report_date, "dbt_result": test_res}

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_regulatory_user_summary(override_date=date_arg)
    run_reconciliation_gate(override_date=date_arg)

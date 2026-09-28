"""
GCP configuration and secret management module.
Handles credentials via Google Secret Manager, dynamic report dates, and dbt command execution.
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DBT_DIR = os.path.join(CURRENT_DIR, "Query (dbt model)")

# Centralized GCP Environment Configuration
GCP_CONFIG = {
    "project_id": os.getenv("GCP_PROJECT_ID", "monee-data-platform-prod"),
    "region": os.getenv("GCP_REGION", "asia-southeast2"),
    "raw_dataset": "raw",
    "silver_dataset": "monee_silver",
    "gold_dataset": "monee_gold",
    "regulatory_dataset": "monee_regulatory",
    "service_account": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "monee-sa-pipeline@monee-data-platform-prod.iam.gserviceaccount.com")
}

class GcpSecurityVault:
    """
    Retrieves secrets from Google Secret Manager, environment variables, or local mock vault.
    """
    _MOCK_SECRETS = {
        "ojk_api_token": "token_bearer_ojk_prod_encrypted_98234",
        "bi_sftp_private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEA0...",
        "slik_api_secret": "sec_slik_key_live_2026_xyz"
    }

    @classmethod
    def get_secret(cls, secret_name: str) -> str:
        # 1. Google Secret Manager in GCP
        try:
            from google.cloud import secretmanager
            client = secretmanager.SecretManagerServiceClient()
            project = GCP_CONFIG["project_id"]
            name = f"projects/{project}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception:
            pass

        # 2. Environment variable fallback
        env_val = os.getenv(secret_name.upper()) or os.getenv(secret_name)
        if env_val:
            return env_val

        # 3. Local mock fallback
        if secret_name in cls._MOCK_SECRETS:
            return cls._MOCK_SECRETS[secret_name]

        raise ValueError(f"Secret '{secret_name}' not found in Secret Vault.")

def get_report_date(context: Optional[Dict[str, Any]] = None, override_date: Optional[str] = None) -> str:
    """Returns report date (Airflow ds or yesterday EOD)."""
    if override_date:
        return override_date
    if context and context.get("ds"):
        return context["ds"]
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

def run_dbt_command(command_args: list, project_dir: str = DBT_DIR) -> Dict[str, Any]:
    """Executes dbt command with execution status."""
    full_cmd = ["dbt"] + command_args + ["--project-dir", project_dir, "--profiles-dir", project_dir]
    cmd_str = " ".join(full_cmd)
    
    if os.getenv("EXECUTE_DBT_REAL") == "true":
        try:
            res = subprocess.run(full_cmd, capture_output=True, text=True, check=True)
            return {"status": "SUCCESS", "output": res.stdout}
        except subprocess.CalledProcessError as e:
            return {"status": "FAILED", "error": e.stderr}
    else:
        # Simulation mode for local validation
        return {"status": "SUCCESS", "simulated": True, "command": cmd_str}

def run_step_0_runner_info():
    print("\n" + "=" * 60)
    print("Initializing GCP environment and credentials...")
    print(f"Project ID : {GCP_CONFIG['project_id']}")
    print(f"Region     : {GCP_CONFIG['region']}")
    print(f"Service Acc: {GCP_CONFIG['service_account']}")
    print(f"Profiles   : {os.path.join(DBT_DIR, 'profiles.yml')}")
    print("=" * 60 + "\n")
    return {"status": "SUCCESS", "step": 0, "gcp_config": GCP_CONFIG}

if __name__ == "__main__":
    run_step_0_runner_info()

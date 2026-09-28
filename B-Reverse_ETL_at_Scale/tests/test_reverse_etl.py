"""
Unit Tests for Reverse-ETL System.
Validates modular functionality: config loading, secret management,
schema transformation, and pipeline execution.
"""

import unittest
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.core.config_loader import ConfigLoader
from src.core.secret_vault import SecretVault
from src.core.state_store import StateManager
from src.runner import ReverseEtlRunner

class TestReverseEtl(unittest.TestCase):
    def setUp(self):
        self.configs_dir = os.path.join(BASE_DIR, "configs")
        self.state_file = "/tmp/test_reverse_etl_state.json"
        if os.path.exists(self.state_file):
            os.remove(self.state_file)

    def tearDown(self):
        if os.path.exists(self.state_file):
            os.remove(self.state_file)

    def test_discover_configs(self):
        """Ensures all YAML configuration files are parsed and valid."""
        configs = ConfigLoader.discover_all_configs(self.configs_dir)
        self.assertGreaterEqual(len(configs), 2, "Must find at least 2 config files (OJK & BI)")
        names = [c.pipeline_name for c in configs]
        self.assertIn("ojk_daily_loan_performance", names)
        self.assertIn("bi_daily_user_summary", names)

    def test_secret_vault(self):
        """Ensures Secret Vault retrieves tokens without hardcoded plain-text."""
        vault = SecretVault()
        token = vault.get_secret("ojk_api_token")
        self.assertTrue(len(token) > 0)
        with self.assertRaises(ValueError):
            vault.get_secret("non_existent_key")

    def test_state_manager_watermarking(self):
        """Ensures watermarks are persisted and retrievable."""
        sm = StateManager(storage_path=self.state_file)
        self.assertEqual(sm.get_watermark("test_pipe", default="2026-09-01"), "2026-09-01")
        sm.commit_watermark("test_pipe", "2026-09-12", 100)
        self.assertEqual(sm.get_watermark("test_pipe"), "2026-09-12")

    def test_pipeline_execution(self):
        """Ensures the runner successfully executes the OJK API pipeline."""
        cfg_path = os.path.join(self.configs_dir, "ojk_daily_loans.yaml")
        cfg = ConfigLoader.load_from_file(cfg_path)
        runner = ReverseEtlRunner(state_store_path=self.state_file)
        result = runner.run_pipeline(cfg, execution_date="2026-09-12")
        self.assertEqual(result["status"], "SUCCESS")
        self.assertGreater(result["records_synced"], 0)

if __name__ == "__main__":
    unittest.main()

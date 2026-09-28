"""
Core runner for Reverse-ETL pipelines.
Handles config loading, warehouse extraction, schema transformation,
batch dispatching, and state watermarking.
"""

import sys
import os
import argparse
from typing import List, Dict, Any

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.core.config_loader import ConfigLoader, PipelineConfig
from src.core.secret_vault import SecretVault
from src.core.state_store import StateManager
from src.destinations.api_destination import RestApiDestination
from src.destinations.sftp_destination import SftpDestination

class ReverseEtlRunner:
    def __init__(self, state_store_path: str = "/tmp/reverse_etl_state.json"):
        self.secret_vault = SecretVault()
        self.state_manager = StateManager(storage_path=state_store_path)

    def _get_destination_client(self, config: PipelineConfig):
        secret_token = self.secret_vault.get_secret(config.secret_key_ref)
        if config.destination_type == "rest_api":
            return RestApiDestination(config.destination_config, secret_token)
        elif config.destination_type == "sftp":
            return SftpDestination(config.destination_config, secret_token)
        else:
            raise ValueError(f"Unsupported destination type: {config.destination_type}")

    def _mock_warehouse_query(self, query: str, watermark_date: str) -> List[Dict[str, Any]]:
        """Mock extraction from BigQuery matching Output A or Output B schemas."""
        if "output_a" in query or "loan_performance" in query or "daily_loan" in query:
            return [
                {
                    "report_date": watermark_date,
                    "loan_id": 1001,
                    "user_id": 5001,
                    "product_type": "P01",
                    "loan_status": "ACTIVE",
                    "principal_amount": 10000000.0,
                    "total_paid": 2500000.0,
                    "outstanding_principal": 7500000.0
                },
                {
                    "report_date": watermark_date,
                    "loan_id": 1002,
                    "user_id": 5002,
                    "product_type": "P02",
                    "loan_status": "OVERDUE",
                    "principal_amount": 5000000.0,
                    "total_paid": 1000000.0,
                    "outstanding_principal": 4000000.0
                }
            ]
        else:
            return [
                {
                    "report_date": watermark_date,
                    "user_id": 5001,
                    "name": "Budi Santoso",
                    "risk_segment": "LOW",
                    "active_loan_count": 1,
                    "overdue_loan_count": 0,
                    "total_principal": 10000000.0,
                    "total_outstanding": 7500000.0
                }
            ]

    def _transform_records(self, records: List[Dict[str, Any]], mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """Maps record column names to destination contract."""
        transformed = []
        for row in records:
            mapped_row = {}
            for src_col, val in row.items():
                dest_col = mapping.get(src_col, src_col)
                mapped_row[dest_col] = val
            transformed.append(mapped_row)
        return transformed

    def run_pipeline(self, config: PipelineConfig, execution_date: str = None, force: bool = False) -> Dict[str, Any]:
        pipeline_name = config.pipeline_name
        print(f"\nRunning pipeline: {pipeline_name}")

        # 1. Determine watermark date
        current_watermark = execution_date or self.state_manager.get_watermark(pipeline_name, default="2026-09-12")
        print(f"Watermark date: {current_watermark}")

        # 1.1 Skip if already synchronized
        if not force and self.state_manager.is_date_synced(pipeline_name, current_watermark):
            print(f"Date {current_watermark} already synced for {pipeline_name}, skipping.")
            return {
                "pipeline_name": pipeline_name,
                "status": "SKIPPED_ALREADY_SYNCED",
                "watermark": current_watermark,
                "records_synced": 0,
                "resource_consumed": "0 bytes"
            }

        # 2. Destination client & health check
        destination = self._get_destination_client(config)
        if not destination.health_check():
            raise ConnectionError(f"Health check failed for destination: {config.destination_type}")

        # 3. Extract delta from warehouse
        raw_records = self._mock_warehouse_query(config.source_query, current_watermark)
        print(f"Extracted {len(raw_records)} records from warehouse.")

        if not raw_records:
            print("No new records to process.")
            return {"status": "SUCCESS", "records_synced": 0}

        # 4. Transform schema
        transformed_records = self._transform_records(raw_records, config.mapping)

        # 5. Dispatch batches
        batch_size = config.batch_size
        total_synced = 0
        for i in range(0, len(transformed_records), batch_size):
            batch = transformed_records[i:i + batch_size]
            batch_id = f"{pipeline_name}_{current_watermark}_batch_{i // batch_size + 1}"
            result = destination.send_batch(batch, batch_id)
            total_synced += len(batch)
            print(f"Batch {batch_id} delivered (status: {result.get('status')})")

        destination.close()

        # 6. Commit watermark
        self.state_manager.commit_watermark(pipeline_name, current_watermark, total_synced)
        print(f"Pipeline {pipeline_name} completed, {total_synced} records synced.\n")

        return {
            "status": "SUCCESS",
            "pipeline": pipeline_name,
            "records_synced": total_synced,
            "watermark": current_watermark
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reverse-ETL Engine CLI")
    parser.add_argument("--config", type=str, help="Path to YAML config file")
    parser.add_argument("--date", type=str, default="2026-09-12", help="Report date (YYYY-MM-DD)")
    args = parser.parse_args()

    configs_dir = os.path.join(BASE_DIR, "configs")
    if args.config:
        target_config = ConfigLoader.load_from_file(args.config)
        runner = ReverseEtlRunner()
        runner.run_pipeline(target_config, args.date)
    else:
        # Run all active pipelines in configs/ directory
        configs = ConfigLoader.discover_all_configs(configs_dir)
        runner = ReverseEtlRunner()
        for cfg in configs:
            runner.run_pipeline(cfg, args.date)

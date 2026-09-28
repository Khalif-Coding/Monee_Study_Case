"""
Loads and validates pipeline YAML configuration files.
"""

import os
import yaml
from dataclasses import dataclass
from typing import Dict, Any, List

@dataclass
class PipelineConfig:
    pipeline_name: str
    description: str
    schedule: str
    enabled: bool
    source_query: str
    incremental_strategy: str
    watermark_column: str
    batch_size: int
    mapping: Dict[str, str]
    destination_type: str
    destination_config: Dict[str, Any]
    secret_key_ref: str
    max_retries: int
    backoff_factor: float
    dlq_enabled: bool

class ConfigLoader:
    @staticmethod
    def load_from_file(file_path: str) -> PipelineConfig:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Config file not found: {file_path}")

        with open(file_path, "r") as f:
            raw = yaml.safe_load(f)

        # Basic schema validation
        required_keys = ["pipeline_name", "source", "mapping", "destination"]
        for key in required_keys:
            if key not in raw:
                raise ValueError(f"Missing required config key '{key}' in {file_path}")

        dest = raw["destination"]
        auth = dest.get("auth", {})
        reliability = raw.get("reliability", {})
        incremental = raw.get("incremental", {})

        return PipelineConfig(
            pipeline_name=raw["pipeline_name"],
            description=raw.get("description", ""),
            schedule=raw.get("schedule", "0 0 * * *"),
            enabled=raw.get("enabled", True),
            source_query=raw["source"]["query"],
            incremental_strategy=incremental.get("strategy", "date_watermark"),
            watermark_column=incremental.get("watermark_column", "report_date"),
            batch_size=incremental.get("batch_size", 500),
            mapping=raw["mapping"],
            destination_type=dest["type"],
            destination_config=dest,
            secret_key_ref=auth.get("secret_key_ref", ""),
            max_retries=reliability.get("max_retries", 3),
            backoff_factor=reliability.get("backoff_factor", 2.0),
            dlq_enabled=reliability.get("dlq_enabled", True)
        )

    @staticmethod
    def discover_all_configs(configs_dir: str) -> List[PipelineConfig]:
        """Discovers and parses all active pipeline YAML files within configs directory."""
        configs = []
        if not os.path.isdir(configs_dir):
            return configs

        for filename in sorted(os.listdir(configs_dir)):
            if filename.endswith(".yaml") or filename.endswith(".yml"):
                cfg = ConfigLoader.load_from_file(os.path.join(configs_dir, filename))
                if cfg.enabled:
                    configs.append(cfg)
        return configs

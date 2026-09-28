"""
State and watermark manager for incremental sync tracking.
"""

import json
import os
from datetime import datetime, timezone
from typing import Optional

class StateManager:
    def __init__(self, storage_path: str = "/tmp/reverse_etl_state.json"):
        self.storage_path = storage_path
        self._state = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, "r") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save(self):
        with open(self.storage_path, "w") as f:
            json.dump(self._state, f, indent=2)

    def get_watermark(self, pipeline_name: str, default: Optional[str] = None) -> str:
        """Retrieves last processed date/time watermark."""
        return self._state.get(pipeline_name, {}).get("last_watermark", default or "2026-09-11")

    def is_date_synced(self, pipeline_name: str, report_date: str) -> bool:
        """Checks if this specific report date was already successfully synchronized."""
        pipeline_state = self._state.get(pipeline_name, {})
        synced_dates = pipeline_state.get("synced_dates", [])
        return report_date in synced_dates

    def commit_watermark(self, pipeline_name: str, watermark_val: str, records_synced: int):
        """Updates and commits watermark after successful batch delivery."""
        if pipeline_name not in self._state:
            self._state[pipeline_name] = {}
            
        synced_dates = self._state[pipeline_name].get("synced_dates", [])
        if watermark_val not in synced_dates:
            synced_dates.append(watermark_val)

        self._state[pipeline_name].update({
            "last_watermark": watermark_val,
            "synced_dates": synced_dates,
            "last_success_at": datetime.now(timezone.utc).isoformat(),
            "records_synced": records_synced
        })
        self._save()

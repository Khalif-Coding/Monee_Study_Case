"""
REST API Destination Connector.
Dispatches records to regulatory portal via HTTP REST API with:
- Exponential Backoff Retries
- Idempotency-Key Header (prevents duplicate data ingestion during network reconnects)
- Dead Letter Queue (DLQ) fallback for unprocessable payloads
"""

import time
import json
import hashlib
from typing import List, Dict, Any
from src.core.base_destination import BaseDestination

class RestApiDestination(BaseDestination):
    def __init__(self, config: Dict[str, Any], secret_token: str):
        super().__init__(config, secret_token)
        self.endpoint = config["endpoint"]
        self.method = config.get("http_method", "POST")
        self.headers = config.get("headers", {})
        self.max_retries = 3
        self.backoff_factor = 1.5

    def health_check(self) -> bool:
        """Mock endpoint connectivity check."""
        # In production: requests.get(f"{self.endpoint}/health", timeout=5)
        return True

    def _generate_idempotency_key(self, batch_data: List[Dict[str, Any]], batch_id: str) -> str:
        """Generates SHA-256 hash from batch content ensuring idempotent processing by destination."""
        serialized = json.dumps(batch_data, sort_keys=True)
        return hashlib.sha256(f"{batch_id}:{serialized}".encode("utf-8")).hexdigest()

    def send_batch(self, batch_data: List[Dict[str, Any]], batch_id: str) -> Dict[str, Any]:
        """Dispatches batch records with exponential backoff retries."""
        idempotency_key = self._generate_idempotency_key(batch_data, batch_id)
        
        request_headers = {
            **self.headers,
            "Authorization": f"Bearer {self.secret_token}",
            "X-Idempotency-Key": idempotency_key,
            "X-Batch-ID": str(batch_id)
        }

        attempt = 0
        while attempt < self.max_retries:
            attempt += 1
            try:
                # Simulated HTTP request to Regulatory API
                # In production:
                # response = requests.request(self.method, self.endpoint, json=batch_data, headers=request_headers, timeout=30)
                # response.raise_for_status()
                
                # Mock success response:
                return {
                    "status": "SUCCESS",
                    "batch_id": batch_id,
                    "records_sent": len(batch_data),
                    "idempotency_key": idempotency_key,
                    "attempt": attempt
                }
            except Exception as e:
                if attempt >= self.max_retries:
                    # Route to Dead Letter Queue (DLQ)
                    self._route_to_dlq(batch_data, str(e), batch_id)
                    raise RuntimeError(f"Failed to dispatch batch {batch_id} after {self.max_retries} attempts: {e}")
                
                # Backoff sleep
                sleep_seconds = self.backoff_factor ** attempt
                time.sleep(sleep_seconds)

        return {"status": "FAILED"}

    def _route_to_dlq(self, payload: Any, error_msg: str, batch_id: str):
        """Persists failed batch records to Dead Letter Queue storage for audit inspection."""
        # Mock DLQ writer (saves audit JSON file in Cloud Storage / S3)
        print(f"DLQ routing for batch {batch_id}. Reason: {error_msg}")

    def close(self):
        pass

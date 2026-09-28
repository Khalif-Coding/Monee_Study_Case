"""
Abstract Base Class for Reverse-ETL Destinations.
Any new destination connector (REST API, SFTP, S3, Kafka, Webhook) extends this base class.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseDestination(ABC):
    def __init__(self, config: Dict[str, Any], secret_token: str):
        self.config = config
        self.secret_token = secret_token

    @abstractmethod
    def health_check(self) -> bool:
        """Verifies endpoint reachability before querying large warehouse datasets."""
        pass

    @abstractmethod
    def send_batch(self, batch_data: List[Dict[str, Any]], batch_id: str) -> Dict[str, Any]:
        """
        Dispatches a single batch of records to the destination portal.
        Must guarantee idempotency (safe to retry without creating duplicate records).
        """
        pass

    @abstractmethod
    def close(self):
        """Cleans up sockets, sessions, or connections."""
        pass

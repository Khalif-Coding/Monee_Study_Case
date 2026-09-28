"""
Connector for Secret Manager integration.
"""

import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PART_B_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
WORKSPACE_ROOT = os.path.dirname(PART_B_DIR)
PART_A_DIR = os.path.join(WORKSPACE_ROOT, "A-Data_Solution_Design")

for p in [WORKSPACE_ROOT, PART_A_DIR, PART_B_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

try:
    from GCP_Config import GcpSecurityVault
except ImportError:
    class GcpSecurityVault:
        _MOCK_SECRETS = {
            "ojk_api_token": "token_bearer_ojk_prod_encrypted_98234",
            "bi_sftp_private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEA0...",
            "slik_api_secret": "sec_slik_key_live_2026_xyz"
        }
        @classmethod
        def get_secret(cls, secret_name: str) -> str:
            val = os.getenv(secret_name.upper()) or os.getenv(secret_name)
            if val:
                return val
            if secret_name in cls._MOCK_SECRETS:
                return cls._MOCK_SECRETS[secret_name]
            raise ValueError(f"Secret '{secret_name}' not found in Secret Vault.")

class SecretVault:
    """Delegates secret retrieval to GCP Security Vault."""
    def __init__(self):
        pass

    def get_secret(self, secret_key_ref: str) -> str:
        return GcpSecurityVault.get_secret(secret_key_ref)

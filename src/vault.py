"""Vault client module for Mode 2: AppRole auth + dynamic credentials.

Module-level state holds the current Vault credentials so that api.py can
switch modes without passing credentials around.
"""
import json
import os
import time
from dataclasses import dataclass

import hvac

# Path where vault-init writes role_id + secret_id (mounted Docker volume)
_CREDENTIALS_FILE = "/vault/init/credentials.json"

VAULT_ADDR = os.environ.get("VAULT_ADDR", "http://vault:8200")


@dataclass
class VaultCredentials:
    vault_token: str
    token_ttl: int          # seconds reported by Vault at login
    db_username: str
    db_password: str
    db_ttl: int             # seconds reported by Vault at lease creation
    fetched_at: float       # time.time() when creds were fetched


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
_current_creds: VaultCredentials | None = None
_anthropic_key: str | None = None
_svid_info: dict | None = None


def _decode_jwt_payload(token: str) -> dict:
    """Base64url-decode JWT middle segment and return parsed JSON, {} on error."""
    import base64
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload = parts[1]
        # Add padding
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return {}


def initialize_mode2() -> VaultCredentials:
    """Authenticate to Vault via AppRole, fetch DB creds + Anthropic key.

    Reads role_id/secret_id from the shared Docker volume written by vault-init.
    Stores results in module-level state. Raises on any Vault error.
    """
    global _current_creds, _anthropic_key

    # Load AppRole credentials from the shared volume
    if not os.path.exists(_CREDENTIALS_FILE):
        raise RuntimeError(
            f"Vault credentials file not found at {_CREDENTIALS_FILE}. "
            "Is vault-init healthy?"
        )

    with open(_CREDENTIALS_FILE) as f:
        approle_creds = json.load(f)

    role_id = approle_creds["role_id"]
    secret_id = approle_creds["secret_id"]

    client = hvac.Client(url=VAULT_ADDR)

    # AppRole login
    login_resp = client.auth.approle.login(
        role_id=role_id,
        secret_id=secret_id,
    )
    vault_token = login_resp["auth"]["client_token"]
    token_ttl = login_resp["auth"]["lease_duration"]

    # Use the issued token for subsequent requests
    client.token = vault_token

    # Fetch dynamic Postgres credentials
    db_resp = client.secrets.database.generate_credentials(name="cricket-writer")
    db_username = db_resp["data"]["username"]
    db_password = db_resp["data"]["password"]
    db_ttl = db_resp["lease_duration"]

    # Fetch Anthropic API key from KV v2
    kv_resp = client.secrets.kv.v2.read_secret_version(
        path="anthropic/api_key",
        mount_point="secret",
    )
    anthropic_key = kv_resp["data"]["data"]["key"]

    _anthropic_key = anthropic_key
    _current_creds = VaultCredentials(
        vault_token=vault_token,
        token_ttl=token_ttl,
        db_username=db_username,
        db_password=db_password,
        db_ttl=db_ttl,
        fetched_at=time.time(),
    )

    return _current_creds


def initialize_mode1() -> VaultCredentials:
    """Authenticate to Vault via SPIFFE JWT-SVID. 15-minute TTLs.

    Flow: SPIRE workload API → JWT-SVID → Vault JWT auth → DB creds + Anthropic key.
    Requires SPIFFE_ENDPOINT_SOCKET env var (e.g. unix:///spire/sockets/api.sock).
    """
    global _current_creds, _anthropic_key, _svid_info

    from spiffe import WorkloadApiClient

    socket_path = os.environ.get("SPIFFE_ENDPOINT_SOCKET")
    if not socket_path:
        raise RuntimeError("SPIFFE_ENDPOINT_SOCKET is not set.")

    with WorkloadApiClient() as spiffe_client:
        jwt_svids = spiffe_client.fetch_jwt_svids(["vault"])
    jwt_token = jwt_svids[0].token

    # Decode SVID claims for metadata
    claims = _decode_jwt_payload(jwt_token)
    issued_at = claims.get("iat", 0)
    expires_at = claims.get("exp", 0)
    ttl_total = expires_at - issued_at if (issued_at and expires_at) else 900
    _svid_info = {
        "spiffe_id": claims.get("sub", "spiffe://example.org/cricket-agent"),
        "trust_domain": claims.get("sub", "").split("//")[-1].split("/")[0] if claims.get("sub") else "example.org",
        "issued_at": issued_at,
        "expires_at": expires_at,
        "ttl_total": ttl_total,
    }

    client = hvac.Client(url=VAULT_ADDR)
    login_resp = client.auth.jwt.jwt_login(role="cricket-agent", jwt=jwt_token)
    vault_token = login_resp["auth"]["client_token"]
    token_ttl = login_resp["auth"]["lease_duration"]
    client.token = vault_token

    db_resp = client.secrets.database.generate_credentials(name="cricket-writer-short")
    db_username = db_resp["data"]["username"]
    db_password = db_resp["data"]["password"]
    db_ttl = db_resp["lease_duration"]

    kv_resp = client.secrets.kv.v2.read_secret_version(
        path="anthropic/api_key",
        mount_point="secret",
    )
    anthropic_key = kv_resp["data"]["data"]["key"]

    _anthropic_key = anthropic_key
    _current_creds = VaultCredentials(
        vault_token=vault_token,
        token_ttl=token_ttl,
        db_username=db_username,
        db_password=db_password,
        db_ttl=db_ttl,
        fetched_at=time.time(),
    )
    return _current_creds


def get_current_creds() -> VaultCredentials | None:
    """Return the current Vault credentials, or None if not in Mode 2."""
    return _current_creds


def get_anthropic_key() -> str:
    """Return Vault-sourced Anthropic key if available, else fall back to env var."""
    if _anthropic_key is not None:
        return _anthropic_key
    return os.environ["ANTHROPIC_API_KEY"]


def get_svid_info() -> dict | None:
    """Return SVID metadata from Mode 1, or None."""
    return _svid_info


def clear() -> None:
    """Reset Vault credential state (called when reverting to Mode 3)."""
    global _current_creds, _anthropic_key, _svid_info
    _current_creds = None
    _anthropic_key = None
    _svid_info = None

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineSettings(BaseSettings):
    """
    Type-safe settings loaded from environment variables.

    Production: all non-secret values injected by Cloud Composer
    env_variables block in terraform/modules/composer/main.tf.

    Secrets: materialised to /run/secrets/ by docker/entrypoint.sh
    from GCP Secret Manager at container startup.
    """
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # SFTP
    SFTP_HOST: str = Field(..., description="Partner SFTP hostname")
    SFTP_PORT: int = Field(22)
    SFTP_USER: str = Field(..., description="SFTP username")
    SFTP_SSH_KEY_PATH: Path = Field(
        Path("/run/secrets/sftp_rsa_key"),
        description=(
            "Path where entrypoint.sh materialises the SFTP RSA key "
            "from Secret Manager secret: sftp-ssh-private-key"
        ),
    )
    SFTP_KNOWN_HOSTS: Path = Field(
        default_factory=lambda: Path.home() / ".ssh" / "known_hosts"
    )


    # SFTP directory config
    # NEW: path to the YAML file that defines all SFTP directories.
    # Injected by Composer env_variables as SFTP_DIRECTORIES_YAML.
    # Also set in Dockerfile.prod as ENV SFTP_DIRECTORIES_YAML=...
    SFTP_DIRECTORIES_YAML: Path = Field(
        Path("manifests/sftp_directories.yaml"),
        description=(
            "Path to sftp_directories.yaml. Loaded by directory_config.py "
            "to discover all SFTP directories to ingest."
        ),
    )

    # PGP
    PGP_PUBLIC_KEY_PATH: Path = Field(
        Path("keys/public_key.asc"),
        description="Shared public key committed to repo. Safe to version-control.",
    )
    PGP_PRIVATE_KEY_PATH: Path = Field(
        Path("/run/secrets/pgp_private_key.asc"),
        description=(
            "Global PGP private key path. Used when a directory does not "
            "have a per-directory key. For multi-key setups each directory "
            "uses /run/secrets/{dir_id}_pgp_private_key.asc instead."
        ),
    )
    PGP_PASSPHRASE: SecretStr = Field(
        ...,
        description=(
            "Global PGP passphrase. For per-directory passphrases, "
            "get_dir_pgp_passphrase(dir_id) reads {DIR_ID}_PGP_PASSPHRASE."
        ),
    )

    PGP_GNUPGHOME: Path = Field(Path("/tmp/gnupg"))

    # GCS
    # Matches env_variable GCS_PROJECT in terraform/modules/composer/main.tf
    GCS_PROJECT: str = Field(..., description="GCP project ID")
    # Matches env_variable GCS_BUCKET in terraform/modules/composer/main.tf
    # Value = var.gcs_bucket_name from terraform.tfvars
    GCS_BUCKET: str = Field(..., description="GCS data lake bucket name")
    # None = Workload Identity via Composer SA (recommended for production)
    GCS_SA_KEY_PATH: Optional[Path] = Field(
        None,
        description="SA key path. None = use Workload Identity.",
    )

    # Validation
    # Matches env_variable CHECKSUM_EXTENSION in terraform/modules/composer/main.tf
    CHECKSUM_EXTENSION: str = Field(".sha256")
    # Matches env_variable MAX_MISSING_FILES_ALLOWED in terraform/modules/composer/main.tf
    MAX_MISSING_FILES_ALLOWED: int = Field(0)
    # EXPECTED_FILES_MANIFEST removed — arrival checking now uses
    # SFTPDirectory.all_expected_files(business_date) from directory_config.py

    # BigQuery Audit
    # Matches google_bigquery_dataset.audit.dataset_id in modules/bigquery/main.tf
    BQ_DATASET: str = Field("pipeline_audit")
    # Matches google_bigquery_table.ingestion_runs.table_id in modules/bigquery/main.tf
    BQ_TABLE:   str = Field("ingestion_runs")

    # BigQuery Gold
    # NEW: matches env_variable BQ_GOLD_DATASET in terraform/modules/composer/main.tf
    # Value = google_bigquery_dataset.gold.dataset_id = "gold_insurance"
    BQ_GOLD_DATASET: str = Field("gold_insurance")

    # Alerting
    SLACK_WEBHOOK_URL: Optional[SecretStr] = Field(None)
    PAGERDUTY_API_KEY: Optional[SecretStr] = Field(None)
    ALERT_EMAIL:       str                 = Field("")

    # Environment
    # Matches env_variable ENVIRONMENT in terraform/modules/composer/main.tf
    # Controls entrypoint.sh secret-fetch logic: "dev" skips Secret Manager
    ENVIRONMENT: str = Field("dev")


# Singleton — imported by all pipeline modules
settings = PipelineSettings()


# Per-directory PGP helpers

def get_dir_pgp_key_path(dir_id: str) -> Path:
    """
    Return the PGP private key path for a given SFTP directory.

    In production, entrypoint.sh materialises per-directory keys:
      /inbound/ → /run/secrets/inbound_pgp_private_key.asc
      (from Secret Manager secret: inbound-pgp-private-key)

    Falls back to the global PGP_PRIVATE_KEY_PATH if the per-directory
    file does not exist (single-key setup or dev environment).
    """
    per_dir_path = Path(f"/run/secrets/{dir_id}_pgp_private_key.asc")
    if per_dir_path.exists():
        return per_dir_path
    # Fallback: global key (dev or single-key prod setup)
    return settings.PGP_PRIVATE_KEY_PATH


def get_dir_pgp_passphrase(dir_id: str) -> str:
    """
    Return the PGP passphrase for a given SFTP directory.

    In production, entrypoint.sh sets per-directory env vars:
      INBOUND_PGP_PASSPHRASE  (for /inbound/)

    Falls back to the global PGP_PASSPHRASE if no per-directory
    env var is set (single-key setup or dev environment).
    """
    env_var = f"{dir_id.upper()}_PGP_PASSPHRASE"
    per_dir_passphrase = os.environ.get(env_var)
    if per_dir_passphrase:
        return per_dir_passphrase
    # Fallback: global passphrase
    return settings.PGP_PASSPHRASE.get_secret_value()

# Secret ID registry

# Global secrets — shared across all SFTP directories
GLOBAL_SECRET_IDS = {
    "sftp_ssh_key":      "sftp-ssh-private-key",
    "sftp_host":         "sftp-host",
    "sftp_user":         "sftp-user",
    "slack_webhook":     "slack-webhook-url",
    "pagerduty_key":     "pagerduty-api-key",
    "airflow_fernet":    "airflow-fernet-key",
    "airflow_webserver": "airflow-webserver-key",
}

# Per-directory PGP secrets — one set per encrypted directory
# dir_id values must match dir_id in manifests/sftp_directories.yaml
# AND match the pgp_path_ids list in terraform/modules/secret_manager/main.tf
PGP_ENCRYPTED_DIR_IDS = ["inbound"]   # add dir_ids here when adding new encrypted dirs

PGP_SECRET_IDS = {
    f"{d}_pgp_key":        f"{d}-pgp-private-key"
    for d in PGP_ENCRYPTED_DIR_IDS
} | {
    f"{d}_pgp_passphrase": f"{d}-pgp-passphrase"
    for d in PGP_ENCRYPTED_DIR_IDS
}

# Combined registry used by dag_utils.get_secret()
SECRET_IDS = {**GLOBAL_SECRET_IDS, **PGP_SECRET_IDS}

# Secret Manager utilities

def load_secret_to_file(secret_id: str, target_path: Path, project_id: str) -> None:
    """
    Fetch a secret from GCP Secret Manager and write it to a tmpfs file.
    Called by docker/entrypoint.sh at container startup.

    secret_id must match a key in terraform/modules/secret_manager/main.tf
    """
    from google.cloud import secretmanager

    client   = secretmanager.SecretManagerServiceClient()
    name     = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(response.payload.data)
    target_path.chmod(0o600)


def get_secret_value(key: str, project_id: str, version: str = "latest",
                     raw_id: bool = False) -> str:
    """
    Fetch a secret string value from GCP Secret Manager.

    key:    a key from SECRET_IDS dict above (e.g. "sftp_ssh_key")
            OR a raw secret ID when raw_id=True (e.g. "inbound-pgp-passphrase")
    raw_id: if True, use key directly as the secret ID (bypasses dict lookup)

    Falls back to env var (SECRET_ID uppercased, hyphens→underscores)
    when Secret Manager is unavailable — local dev without GCP.
    """
    secret_id    = key if raw_id else SECRET_IDS.get(key, key)
    env_fallback = secret_id.upper().replace("-", "_")

    try:
        from google.cloud import secretmanager
        client   = secretmanager.SecretManagerServiceClient()
        name     = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8").strip()
    except Exception as exc:
        fallback = os.environ.get(env_fallback)
        if fallback:
            return fallback
        raise RuntimeError(
            f"Secret '{secret_id}' unavailable from Secret Manager "
            f"and env var '{env_fallback}' is not set."
        ) from exc
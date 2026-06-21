#!/usr/bin/env bash
#   1. SFTP_REMOTE_DIR env var REMOVED — remote directories are now driven
#      by manifests/sftp_directories.yaml, not a single env variable.
#      The entrypoint no longer sets it.
#
#   2. PGP secrets are now fetched PER DIRECTORY based on the use_pgp flag
#      in sftp_directories.yaml. Only directories with use_pgp=true get
#      their PGP key materialised to tmpfs:
#        /inbound/  (use_pgp=true)  → /run/secrets/inbound_pgp_private_key.asc
#        /reports/  (use_pgp=false) → nothing fetched (no PGP key needed)
#
#   3. The Python block reads sftp_directories.yaml directly to know which
#      directories need PGP secrets — no hardcoding of directory names here.
#      Adding a new encrypted directory = add YAML block. This file unchanged.
#
#   4. Global secrets (SFTP RSA key, host, user) remain as before — one
#      set shared across all directories (same server, same credentials).
#
#   5. PGP_GNUPGHOME env var still used and still set by Composer module.

set -euo pipefail

# Global secret IDs - must match terraform/modules/secret_manager/main.tf
readonly SECRET_SFTP_KEY="sftp-ssh-private-key"
readonly SECRET_SFTP_HOST="sftp-host"
readonly SECRET_SFTP_USER="sftp-user"
readonly SECRET_SLACK="slack-webhook-url"

# Global key paths - must match SFTP_SSH_KEY_PATH in pipeline/config.py
readonly PATH_SFTP_KEY="/run/secrets/sftp_rsa_key"

log_info()  { echo "[entrypoint] ℹ  $*"; }
log_ok()    { echo "[entrypoint] ✅ $*"; }
log_warn()  { echo "[entrypoint] ⚠  $*"; }
log_error() { echo "[entrypoint] ❌ $*" >&2; }

log_info "InsureFlow container starting..."
log_info "ENVIRONMENT   : ${ENVIRONMENT:-dev}"
log_info "GCS_PROJECT   : ${GCS_PROJECT:-not-set}"

# Production mode: fetch secrets from GCP Secret Manager
# ENVIRONMENT is injected by terraform/modules/composer/main.tf env_variables.
# In dev, skip Secret Manager and use .env file or shell env vars.
if [[ "${ENVIRONMENT:-dev}" != "dev" ]]; then
    log_info "Production mode — fetching secrets from GCP Secret Manager..."

    # Create tmpfs secrets directory
    mkdir -p /run/secrets
    chmod 700 /run/secrets

    python3 - <<'PYEOF'
import sys
import os
from pathlib import Path

try:
    from google.cloud import secretmanager
    import yaml

    project = os.environ["GCS_PROJECT"]
    client  = secretmanager.SecretManagerServiceClient()

    def fetch_secret_to_file(secret_id: str, target_path: str) -> None:
        """Fetch binary secret from Secret Manager, write to tmpfs."""
        name = f"projects/{project}/secrets/{secret_id}/versions/latest"
        resp = client.access_secret_version(request={"name": name})
        p    = Path(target_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(resp.payload.data)
        p.chmod(0o600)
        print(f"[entrypoint] ✅ {secret_id} → {target_path} ({p.stat().st_size} bytes)")

    def fetch_secret_str(secret_id: str) -> str:
        """Fetch string secret from Secret Manager."""
        name = f"projects/{project}/secrets/{secret_id}/versions/latest"
        resp = client.access_secret_version(request={"name": name})
        return resp.payload.data.decode("utf-8").strip()

    env_lines = []

    # ── 1. Global SFTP secrets (shared across all directories) ───────────────
    # One SFTP server, one RSA key — used by sftp_client.py for all directories
    fetch_secret_to_file("sftp-ssh-private-key", "/run/secrets/sftp_rsa_key")

    if not os.environ.get("SFTP_HOST"):
        env_lines.append(f'SFTP_HOST={fetch_secret_str("sftp-host")}')
    if not os.environ.get("SFTP_USER"):
        env_lines.append(f'SFTP_USER={fetch_secret_str("sftp-user")}')

    # ── 2. Per-directory PGP secrets (only for use_pgp=true directories) ─────
    # Read sftp_directories.yaml to find which directories need PGP secrets.
    # Secret naming: {dir_id}-pgp-private-key and {dir_id}-pgp-passphrase
    # This means adding a new encrypted directory only requires:
    #   a) Adding YAML block with use_pgp=true
    #   b) Running terraform apply to create {dir_id}-pgp-* secrets
    #   c) Populating secrets with gcloud secrets versions add
    # This file does NOT need to change.
    yaml_path = Path(os.environ.get(
        "SFTP_DIRECTORIES_YAML",
        "/opt/airflow/manifests/sftp_directories.yaml"
    ))

    if yaml_path.exists():
        config = yaml.safe_load(yaml_path.read_text())
        pgp_dirs = [
            d for d in config.get("sftp_directories", [])
            if d.get("use_pgp", False)
        ]
        print(f"[entrypoint] Found {len(pgp_dirs)} PGP-encrypted director(ies).")

        for sftp_dir in pgp_dirs:
            dir_id    = sftp_dir["dir_id"]
            key_path  = f"/run/secrets/{dir_id}_pgp_private_key.asc"
            secret_id = f"{dir_id}-pgp-private-key"
            passphrase_secret = f"{dir_id}-pgp-passphrase"

            # Fetch private key file
            fetch_secret_to_file(secret_id, key_path)

            # Fetch passphrase and add to env
            passphrase = fetch_secret_str(passphrase_secret)
            # PGPHandler reads PGP_PASSPHRASE from settings.
            # For multiple encrypted dirs each with different keys,
            # the passphrase is stored per-dir in env as {DIR_ID}_PGP_PASSPHRASE.
            # PGPHandler.process_directory() picks the right one by dir_id.
            env_var_name = f"{dir_id.upper()}_PGP_PASSPHRASE"
            env_lines.append(f"{env_var_name}={passphrase}")
            print(f"[entrypoint] ✅ PGP secrets loaded for directory: {dir_id}")
    else:
        print(f"[entrypoint] ⚠ {yaml_path} not found — skipping per-dir PGP setup.")

    # ── 3. Optional Slack webhook ──────────────────────────────────────────────
    if not os.environ.get("SLACK_WEBHOOK_URL"):
        try:
            env_lines.append(f'SLACK_WEBHOOK_URL={fetch_secret_str("slack-webhook-url")}')
        except Exception:
            print("[entrypoint] slack-webhook-url not found — Slack alerts disabled.")

    # Write env vars to temp file for shell sourcing
    if env_lines:
        with open("/tmp/runtime_secrets.env", "w") as f:
            f.write("\n".join(env_lines) + "\n")

    print("[entrypoint] ✅ All secrets loaded.")

except Exception as e:
    print(f"[entrypoint] ❌ Secret loading FAILED: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF

    # Source runtime env vars into current shell
    if [[ -f /tmp/runtime_secrets.env ]]; then
        set -a
        # shellcheck disable=SC1091
        source /tmp/runtime_secrets.env
        set +a
        rm -f /tmp/runtime_secrets.env
        log_ok "Runtime env vars sourced."
    fi

else
    log_info "Dev mode — Secret Manager skipped. Using .env file / shell env vars."
fi

#  GPG home setup
# PGP_GNUPGHOME set by terraform/modules/composer/main.tf env_variables = /tmp/gnupg
# PGPHandler uses this as the base directory for isolated keyring per directory.
GPG_HOME="${PGP_GNUPGHOME:-/tmp/gnupg}"
mkdir -p "${GPG_HOME}"
chmod 700 "${GPG_HOME}"
log_ok "GPG home: ${GPG_HOME}"

log_ok "Startup complete. Starting: $*"
exec "$@"
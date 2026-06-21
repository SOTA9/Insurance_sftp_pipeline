# Secret IDs here MUST match pipeline/config.py GLOBAL_SECRET_IDS / PGP_SECRET_IDS
# and docker/entrypoint.sh. This is the single source of truth for naming.
#
# Global secrets — shared across all SFTP directories (one server, one login):
# sftp-ssh-private-key, sftp-host, sftp-user,
# slack-webhook-url, pagerduty-api-key,
# airflow-fernet-key, airflow-webserver-key
#
# Per-directory PGP secrets — one pair per dir_id in var.pgp_encrypted_dir_ids
# (must match PGP_ENCRYPTED_DIR_IDS in pipeline/config.py and
# use_pgp: true entries in manifests/sftp_directories.yaml):
# {dir_id}-pgp-private-key
# {dir_id}-pgp-passphrase
#
# NOTE: this module creates empty secret containers + IAM bindings only.
# It does NOT populate secret values — that's a deliberate split so secret
# material never passes through `terraform plan/apply` state or VCS history.
# Populate values out-of-band with:
# echo -n "VALUE" | gcloud secrets versions add SECRET_ID --data-file=- --project=PROJECT_ID
# or for the SSH/PGP private keys:
# gcloud secrets versions add SECRET_ID --data-file=./key.pem --project=PROJECT_ID

locals {
  global_secret_ids = [
    "sftp-ssh-private-key",
    "sftp-host",
    "sftp-user",
    "slack-webhook-url",
    "pagerduty-api-key",
    "airflow-fernet-key",
    "airflow-webserver-key",
  ]

  # Flatten {dir_id}-pgp-private-key / {dir_id}-pgp-passphrase for every
  # encrypted directory, e.g. ["inbound"] →
  # ["inbound-pgp-private-key", "inbound-pgp-passphrase"]
  pgp_secret_ids = flatten([
    for dir_id in var.pgp_encrypted_dir_ids : [
      "${dir_id}-pgp-private-key",
      "${dir_id}-pgp-passphrase",
    ]
  ])

  all_secret_ids = toset(concat(
    local.global_secret_ids,
    local.pgp_secret_ids,
  ))
}

resource "google_secret_manager_secret" "secret" {
  for_each = local.all_secret_ids

  project   = var.project_id
  secret_id = each.value

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  labels = var.labels
}

# Grant the Composer/Airflow worker service account read access to every
# secret. Required because AIRFLOW__SECRETS__BACKEND_KWARGS in
# docker-compose.yml points at CloudSecretManagerBackend, and
# docker/entrypoint.sh calls access_secret_version() directly at startup.

resource "google_secret_manager_secret_iam_member" "composer_sa_accessor" {
  for_each = local.all_secret_ids

  project   = var.project_id
  secret_id = google_secret_manager_secret.secret[each.value].secret_id

  role   = "roles/secretmanager.secretAccessor"
  member = "serviceAccount:${var.composer_service_account_email}"
}

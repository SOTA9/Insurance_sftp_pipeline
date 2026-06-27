# InsureFlow DataPipeline

A multi-directory SFTP ingestion pipeline that pulls daily insurance files
(policies, claims, premiums, reinsurance) from a partner SFTP server,
decrypts/validates them, and lands them through a Bronze > Silver > Gold
lakehouse in GCS and BigQuery, orchestrated by Cloud Composer (Airflow).

```
SFTP server  >  Bronze (GCS, raw CSV)  >  Silver (GCS, Parquet)  >  Gold (BigQuery)  >  Dashboard
 /inbound/        bronze/inbound/*          silver/policies/*        dim_policy,         Looker Studio
 /reports/        bronze/reports/*          silver/premiums/*        fact_premiums, ...
```

## Why "multi-directory"

One partner SFTP server exposes two directories with different contracts:

| Directory   | Entities                  | Encrypted (PGP) | Checksums | dir_id      |
|-------------|----------------------------|:---------------:|:---------:|-------------|
| `/inbound/` | policies, claims           |       yes       |    yes    | `inbound`   |
| `/reports/` | premiums, reinsurance      |        no       |     no    | `reports`   |

Everything about a directory  - its remote path, encryption flag, checksum
flag, GCS prefix, state key, and expected filename patterns — is declared
once in [`manifests/sftp_directories.yaml`](manifests/sftp_directories.yaml).
Adding a third directory means adding one YAML block; no DAG or pipeline
code changes are required.

## Architecture at a glance

```
dags/ingestion_dag.py          < Airflow DAG: insurance_sftp_gcs_ingestion
  ├── ingest_inbound (TaskGroup)    - check, download, decrypt, validate, upload (parallel)
  ├── ingest_reports (TaskGroup)    - check, download, copy,    validate, upload (parallel)
  ├── transform_to_silver           - waits for both groups, runs after either has new data
  ├── load_to_gold                  - dim/fact/agg tables in BigQuery
  └── write_audit_record            - pipeline_audit.ingestion_runs

pipeline/
  ├── config.py                 < PipelineSettings (env-driven), Secret Manager helpers
  ├── directory_config.py       < loads sftp_directories.yaml into SFTPDirectory objects
  ├── sftp_client.py            < one SSH connection, visits multiple remote directories
  ├── pgp_handler.py            < per-directory GPG keyring isolation, decrypt/encrypt
  ├── checksum_validator.py     < .sha256 sidecar verification (skipped if use_checksum=False)
  ├── data_validator.py         < Pandera schema validation per entity
  ├── file_arrival_checker.py   < expected-vs-actual file comparison before processing
  ├── gcs_uploader.py           < bronze upload, dead-letter routing
  ├── audit_logger.py           < per-file audit trail → BigQuery
  ├── alerting.py               < Slack notifications, dir_id-tagged failure alerts
  ├── state/                    < incremental-load state (see "State management" below)
  │   ├── sftp_state.py             - which SFTP filenames have been ingested
  │   ├── silver_state.py           - which (entity, date) pairs have been transformed
  │   └── gold_state.py             - which (table, date) partitions have been loaded
  └── transforms/
      ├── bronze_to_silver.py   < CSV > partitioned Parquet
      └── silver_to_gold.py     < Parquet > BigQuery dim/fact/agg tables

schemas/                        < Pandera schemas: policies, claims, premiums, reinsurance
manifests/sftp_directories.yaml < single source of truth for directory config
keys/                           < PGP public keys (committed) — see "Secrets" below
terraform/                      < GCS, BigQuery, Secret Manager, Composer, dev/prod envs
tests/                          < pytest suite, one file per pipeline module
```

## Prerequisites

- Python 3.11+
- A GCP project with billing enabled
- `gcloud` CLI authenticated against that project
- Terraform >= 1.7.0
- Access to the partner SFTP server's host key (for `known_hosts`)

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env        # then fill in real values — .env is gitignored, never commit it

mkdir -p keys
python keys/generate_pgp_keypair.py \
  --name "Local Dev" --email "dev@insureflow.io" \
  --passphrase "local-test-passphrase" --output-dir keys

pytest tests/ -v
```

Quick import sanity check (catches the class of bug that broke this
pipeline more than once during development — see "Known footguns" below):

```bash
python3 -c "
from pipeline.state import SFTPState, SilverState, GoldState
from pipeline.pgp_handler import PGPHandler
from pipeline.config import get_dir_pgp_passphrase, get_dir_pgp_key_path
from pipeline.transforms.bronze_to_silver import bronze_to_silver_incremental
print('All imports OK')
"
```

## Infrastructure (Terraform)

```bash
cd terraform
terraform init  -backend-config=environments/dev/backend.hcl
terraform plan  -var-file=environments/dev/terraform.tfvars
terraform apply -var-file=environments/dev/terraform.tfvars
```

This provisions, per environment: the GCS data lake bucket, both BigQuery
datasets (`pipeline_audit`, `gold_insurance`), every Secret Manager secret
container (empty — see "Secrets" below), an Artifact Registry repo for the
Docker image, and the Cloud Composer environment itself.

Repeat with `environments/prod/` for production. The two environments are
fully independent — separate GCP projects, separate state buckets, separate
PGP keys (never reuse a dev key pair in prod).

**One manual step Terraform cannot do for you:** create the two Terraform
state buckets by hand before the first `terraform init`, since a backend
can't create the bucket it stores its own state in:

```bash
gcloud storage buckets create gs://insureflow-tfstate-dev  --project=insureflow-dev-project  --location=us-central1
gcloud storage buckets create gs://insureflow-tfstate-prod --project=insureflow-prod-project --location=us-central1
```

## Secrets

Terraform creates empty secret *containers* only — secret material never
touches `terraform plan/apply` state or version control. Populate values
directly:

```bash
PROJECT=insureflow-dev-project

gcloud secrets versions add sftp-ssh-private-key --data-file=./local-keys/sftp_rsa_key --project=$PROJECT
echo -n "sftp.partner-insurer.com" | gcloud secrets versions add sftp-host --data-file=- --project=$PROJECT
echo -n "insureflow_ingest"        | gcloud secrets versions add sftp-user --data-file=- --project=$PROJECT
echo -n "https://hooks.slack.com/services/XXX" | gcloud secrets versions add slack-webhook-url --data-file=- --project=$PROJECT
```

Per-directory PGP secrets (only directories with `use_pgp: true` —
currently just `inbound`):

```bash
python keys/generate_pgp_keypair.py --dir-id inbound \
    --passphrase "REPLACE_WITH_REAL_PASSPHRASE" --output-dir keys/

gcloud secrets versions add inbound-pgp-private-key --data-file=/tmp/inbound_private_key.asc --project=$PROJECT
echo -n "REPLACE_WITH_REAL_PASSPHRASE" | gcloud secrets versions add inbound-pgp-passphrase --data-file=- --project=$PROJECT
rm -f /tmp/inbound_private_key.asc          # never leave the private key on disk
git add keys/inbound_public_key.asc          # the PUBLIC key is safe to commit
```

Share the committed public key + its fingerprint with the partner so they
can encrypt files to it.

## State management

Three independent JSON-backed state trackers live under `pipeline/state/`,
persisted to `gs://{bucket}/_state/...` so they survive retries and
redeployments:

| Class         | Tracks                                  | Storage layout                                          |
|---------------|------------------------------------------|-----------------------------------------------------------|
| `SFTPState`   | which SFTP filenames were ingested       | **month-partitioned**: `_state/sftp/{dir}/months/{YYYY-MM}.json` + `manifest.json` |
| `SilverState` | which (entity, date) pairs were transformed | single blob: `_state/silver/{entity}/processed_dates.json` |
| `GoldState`   | which (table, date) partitions were loaded | single blob: `_state/gold/{table}/loaded_dates.json` |

`SFTPState` is month-partitioned (not a single ever-growing blob) so that
read/write cost on every DAG run stays bounded by a configurable lookback
window (default: current month + 3 prior), regardless of how many years of
history have accumulated. `SilverState`/`GoldState` store compact date
lists rather than per-file metadata, so they grow far more slowly and have
been left as single blobs — revisit if any one entity's date list grows
past a few thousand entries.

**If you are migrating an existing deployment** from the original
single-blob `SFTPState` format, run the one-time migration *before*
deploying the new code — see `scripts/migrate_sftp_state.py` and run it
with no flags first (dry run) before adding `--apply`.

## Running locally against Airflow

```bash
pip install apache-airflow==2.8.4
export AIRFLOW_HOME=$(pwd)/.airflow
airflow standalone
# DagBag will pick up dags/ingestion_dag.py — confirm no import errors
# in the "DAGs" list before triggering a run.
```

## Deploying

CI/CD is defined in [`.gitlab-ci.yml`](.gitlab-ci.yml): lint → test →
terraform-plan → build → deploy-dev → integration → terraform-apply (manual
gate) → deploy-prod (manual gate) → notify. Merges to `develop` deploy to
dev automatically; merges to `main` build the prod image but require a
manual click to actually apply infrastructure or deploy code to production.

```bash
git push origin develop     # → auto build, deploy, integration-test in dev
git push origin main        # → builds prod image; tf-apply-prod and deploy-prod wait for manual approval
```

## Known footguns (read before touching `pipeline/state/` or `pipeline/pgp_handler.py`)

These bit us during development — recorded here so they don't bite you too.

- **`pipeline/state/` is a *package*, not a module.** An earlier flat
  `pipeline/state.py` file must not coexist with the `pipeline/state/`
  directory — Python silently prefers the package and the flat file
  becomes dead, misleading code. If you ever see both, delete
  `pipeline/state.py`.
- **PGP helper function names are `get_dir_pgp_passphrase` /
  `get_dir_pgp_key_path`** (with the `dir_` infix) in `pipeline/config.py`
  — this matches what `pipeline/pgp_handler.py` and the test suite
  (`test_pgp_handler.py`, `conftest.py`) expect. Renaming these without
  checking the test suite first will pass `py_compile` but break tests.
- **`PGPHandler` must be constructed with `dir_id=...`**
  (`PGPHandler(dir_id=sftp_dir.dir_id)`), not bare `PGPHandler()`, or
  per-directory keyring isolation silently falls back to a shared global
  keyring.
- **`validate_file()` must be called with `sftp_dir`** as the second
  argument, or entity/schema lookup silently falls back to prefix-matching
  against a hardcoded `SCHEMA_MAP` instead of the YAML-driven mapping —
  works by coincidence today, breaks the moment two directories share an
  entity name prefix.
- **The transform function is `bronze_to_silver_incremental`**, not
  `bronze_to_silver` — the latter doesn't exist in
  `pipeline/transforms/bronze_to_silver.py`.
- **`requirements.txt` must include `pyyaml`** — `directory_config.py`,
  `generate_pgp_keypair.py --generate-all`, and `docker/entrypoint.sh`'s
  Python block all import it.

## Testing

```bash
pytest tests/ -v --cov=pipeline --cov=schemas --cov-report=term-missing
```

Test files mirror pipeline modules 1:1 (`test_sftp_client.py`,
`test_pgp_handler.py`, `test_checksum_validator.py`,
`test_data_validator.py`, `test_gcs_uploader.py`,
`test_file_arrival_checker.py`). Shared fixtures, including
`mock_sftp_state_empty` / `mock_sftp_state_with_history`, live in
`conftest.py`.

## Adding a new SFTP directory

1. Add one block to `manifests/sftp_directories.yaml` with a unique
   `dir_id`, `remote_dir`, `use_pgp`/`use_checksum` flags, `gcs_prefix`,
   `state_key`, and a `files` list of `{entity, pattern, checksum, schema}`.
2. If `use_pgp: true`, add the `dir_id` to `PGP_ENCRYPTED_DIR_IDS` in
   `pipeline/config.py` and to `pgp_encrypted_dir_ids` in your Terraform
   `.tfvars`, then run `generate_pgp_keypair.py --dir-id <new_id>` and
   populate its two Secret Manager secrets.
3. Add a matching Pandera schema in `schemas/` if it's a new entity type.
4. No DAG changes needed — `make_directory_group()` builds one TaskGroup
   per YAML block automatically.

## Tech stack

| Layer            | Technology                                          |
|------------------|------------------------------------------------------|
| Orchestration    | Apache Airflow 2.8.4 / Cloud Composer 2              |
| Transport        | Paramiko (SFTP over SSH)                              |
| Encryption       | python-gnupg (PGP/GPG)                                |
| Validation       | Pandera                                               |
| Storage          | Google Cloud Storage (Bronze/Silver), BigQuery (Gold) |
| Secrets          | GCP Secret Manager                                    |
| Infrastructure   | Terraform                                             |
| CI/CD            | GitLab CI                                             |
| Dashboard        | Looker Studio (BigQuery-native, free tier)            |
| Alerting         | Slack webhooks                                        |

## License / ownership

Internal project - add your organization's license terms here.
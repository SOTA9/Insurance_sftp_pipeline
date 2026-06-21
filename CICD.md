# InsureFlow DataPipeline — CI/CD Setup Guide

## Architecture overview

```
feature/* branch  →  MR to develop  →  develop  →  MR to main  →  main
                          ↓                  ↓                       ↓
                       lint+test        build+deploy-dev         build+deploy-prod
                                        integration tests         (manual gate)
```

All secrets live in **GCP Secret Manager** — no plaintext credentials in GitLab variables, Docker images, or `.env` files on production workers.

---

## 1. GitLab CI/CD variables to set

Go to **Settings → CI/CD → Variables** in your GitLab project and add:

| Variable | Type | Protected | Masked | Description |
|---|---|---|---|---|
| `GCP_PROJECT_DEV` | Variable | ✅ | ❌ | GCP project ID for dev |
| `GCP_PROJECT_PROD` | Variable | ✅ | ❌ | GCP project ID for prod |
| `GCP_SA_KEY_DEV` | File | ✅ | ✅ | SA JSON key with dev permissions |
| `GCP_SA_KEY_PROD` | File | ✅ | ✅ | SA JSON key with prod permissions |
| `GCS_BUCKET_DEV` | Variable | ✅ | ❌ | Dev data lake bucket name |
| `GCS_BUCKET_PROD` | Variable | ✅ | ❌ | Prod data lake bucket name |
| `COMPOSER_ENV_DEV` | Variable | ✅ | ❌ | Cloud Composer env name (dev) |
| `COMPOSER_ENV_PROD` | Variable | ✅ | ❌ | Cloud Composer env name (prod) |
| `COMPOSER_LOCATION` | Variable | ✅ | ❌ | GCP region e.g. `europe-west1` |
| `REGISTRY` | Variable | ❌ | ❌ | Artifact Registry host e.g. `europe-west1-docker.pkg.dev` |
| `IMAGE_NAME` | Variable | ❌ | ❌ | e.g. `insureflow-airflow` |
| `SLACK_WEBHOOK_URL` | Variable | ❌ | ✅ | Slack incoming webhook URL |

---

## 2. GCP Secret Manager secrets to create

Run these once per environment (dev and prod separately):

```bash
# Set your project
export PROJECT=your-gcp-project-id

# PGP private key (content of the .asc file, NOT the file path)
gcloud secrets create pgp-private-key --project=$PROJECT
gcloud secrets versions add pgp-private-key \
  --data-file=/tmp/insureflow_private_key.asc \
  --project=$PROJECT

# PGP passphrase
echo -n "your-strong-passphrase" | \
  gcloud secrets versions add pgp-passphrase --data-file=- --project=$PROJECT

# SFTP RSA private key (PEM format)
gcloud secrets create sftp-ssh-private-key --project=$PROJECT
gcloud secrets versions add sftp-ssh-private-key \
  --data-file=~/.ssh/insureflow_sftp_rsa \
  --project=$PROJECT

# Slack webhook (optional)
echo -n "https://hooks.slack.com/services/XXX/YYY/ZZZ" | \
  gcloud secrets versions add slack-webhook-url --data-file=- --project=$PROJECT
```

---

## 3. Service account permissions

The CI/CD service accounts need these IAM roles:

**Dev SA** (`ci-dev@project.iam.gserviceaccount.com`):
- `roles/composer.worker` — deploy DAGs to Composer
- `roles/storage.objectAdmin` — sync files to GCS
- `roles/secretmanager.secretAccessor` — read secrets at runtime
- `roles/artifactregistry.writer` — push Docker images

**Prod SA** (`ci-prod@project.iam.gserviceaccount.com`):
- Same as dev SA but scoped to prod project
- `roles/composer.worker` on prod Composer env

**Airflow worker SA** (runs inside Composer):
- `roles/secretmanager.secretAccessor`
- `roles/bigquery.dataEditor` (write audit rows)
- `roles/storage.objectAdmin` (upload to data lake)

---

## 4. Branch strategy

| Branch | Trigger | Action |
|---|---|---|
| `feature/*` | Push / MR | lint + unit tests only |
| `develop` | Push / merge | lint → test → build-dev → deploy-dev → integration |
| `main` | Push / merge | lint → test → build-prod → **manual gate** → deploy-prod |

---

## 5. Local development workflow

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate test PGP key pair (once)
python keys/generate_pgp_keypair.py

# 3. Copy and fill in env file
cp .env.example .env
# Edit .env with local values (SFTP host, bucket, etc.)

# 4. Spin up local Airflow + mock SFTP
docker compose -f docker/docker-compose.yml up -d

# 5. Run tests
pytest tests/ -v --cov=pipeline --cov-report=term-missing

# 6. Access Airflow UI
open http://localhost:8080  # admin / admin (default dev creds)
```

---

## 6. Promoting to production

1. Merge `develop` → `main` via a merge request.
2. The `build-prod` job runs automatically — image is pushed to Artifact Registry.
3. The `deploy-prod` job appears in the GitLab pipeline UI with a **▶ Play** button.
4. A team lead / release manager clicks **▶ Play** to approve the deployment.
5. GitLab logs `GITLAB_USER_LOGIN` of the approver for audit trail.
6. The prod Composer environment is updated; the image is tagged `prod-stable`.

---

## 7. Rollback

```bash
# Find the last known-good image tag
gcloud container images list-tags \
  ${REGISTRY}/${GCP_PROJECT_PROD}/${IMAGE_NAME} \
  --filter="tags:prod-stable" \
  --format="table(digest,tags,timestamp)"

# Retag a previous image as prod-stable
gcloud container images add-tag \
  ${REGISTRY}/${GCP_PROJECT_PROD}/${IMAGE_NAME}:PREVIOUS_SHA \
  ${REGISTRY}/${GCP_PROJECT_PROD}/${IMAGE_NAME}:prod-stable

# Re-sync the DAGs from the corresponding git tag
git checkout v1.2.3
gsutil -m rsync -r -d dags/ gs://your-composer-bucket/dags/
```
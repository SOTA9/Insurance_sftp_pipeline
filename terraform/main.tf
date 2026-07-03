locals {
  common_labels = merge(var.labels, {
    app         = "insureflow"
    environment = var.environment
    managed_by  = "terraform"
  })
}

# Enable required APIs

resource "google_project_service" "apis" {
  for_each = toset([
    "composer.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "secretmanager.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "artifactregistry.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# Artifact Registry: Docker image repo referenced by .gitlab-ci.yml
# Repository name pattern "insureflow-docker-${var.environment}" is the
# contract .gitlab-ci.yml builds FULL_IMAGE against. Do not rename without
# updating .gitlab-ci.yml's build-dev/build-prod/deploy-prod jobs.

resource "google_artifact_registry_repository" "insureflow_docker" {
  project       = var.project_id
  location      = var.region
  repository_id = "insureflow-docker-${var.environment}"
  format        = "DOCKER"

  description = "InsureFlow Airflow worker image — ${var.environment}"
  labels      = local.common_labels

  depends_on = [google_project_service.apis]
}

resource "google_artifact_registry_repository_iam_member" "composer_sa_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.insureflow_docker.repository_id

  role   = "roles/artifactregistry.reader"
  member = "serviceAccount:${module.composer.composer_service_account_email}"
}

# Composer (creates its own worker service account)

module "composer" {
  source = "./modules/composer"

  project_id              = var.project_id
  region                  = var.region
  environment_name        = "insureflow-${var.environment}"
  environment_tag         = var.environment
  composer_image_version  = var.composer_image_version
  gcs_bucket_name         = var.gcs_bucket_name
  bq_audit_dataset        = var.bq_audit_dataset
  bq_audit_table          = var.bq_audit_table
  bq_gold_dataset         = var.bq_gold_dataset
  environment_size        = var.composer_environment_size
  worker_min_count        = var.worker_min_count
  worker_max_count        = var.worker_max_count
  labels                  = local.common_labels

  depends_on = [google_project_service.apis]
}

# GCS data lake (grants Composer's SA objectAdmin)

module "gcs" {
  source = "./modules/gcs"

  project_id                     = var.project_id
  region                         = var.region
  bucket_name                    = var.gcs_bucket_name
  composer_service_account_email = module.composer.composer_service_account_email
  force_destroy                  = var.bucket_force_destroy
  enable_versioning              = var.environment == "prod"
  labels                         = local.common_labels

  depends_on = [google_project_service.apis]
}

# Secret Manager (grants Composer's SA secretAccessor)

module "secret_manager" {
  source = "./modules/secret_manager"

  project_id                     = var.project_id
  region                         = var.region
  composer_service_account_email = module.composer.composer_service_account_email
  pgp_encrypted_dir_ids          = var.pgp_encrypted_dir_ids
  labels                         = local.common_labels

  depends_on = [google_project_service.apis]
}

# BigQuery (grants Composer's SA dataEditor + jobUser)

module "bigquery" {
  source = "./modules/bigquery"

  project_id                     = var.project_id
  region                         = var.region
  audit_dataset_id               = var.bq_audit_dataset
  audit_table_id                 = var.bq_audit_table
  gold_dataset_id                = var.bq_gold_dataset
  composer_service_account_email = module.composer.composer_service_account_email
  deletion_protection            = var.deletion_protection
  labels                         = local.common_labels

  depends_on = [google_project_service.apis]
}

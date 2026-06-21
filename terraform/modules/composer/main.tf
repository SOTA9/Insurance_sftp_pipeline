# Cloud Composer 2 environment running the InsureFlow DAG.
#
# env_variables below MUST mirror docker-compose.yml's x-airflow-common
# block and pipeline/config.py's PipelineSettings field names exactly —
# these become the env vars the Airflow workers/scheduler/webserver see.

resource "google_service_account" "composer_worker" {
  project      = var.project_id
  account_id   = var.service_account_id
  display_name = "InsureFlow Composer worker SA"
}

# Composer v2 requires the Composer Worker role on its own SA, plus the
# Composer Agent role on the Cloud Composer service agent (handled by the
# google_project_service / API enablement — Composer manages that agent
# binding itself once the API is enabled, so it is not declared here).

resource "google_project_iam_member" "composer_worker_role" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_worker.email}"
}

resource "google_composer_environment" "insureflow" {
  project = var.project_id
  region  = var.region
  name    = var.environment_name

  config {

    software_config {
      image_version = var.composer_image_version

      env_variables = {
        # Pipeline env vars — match x-airflow-common in docker-compose.yml
        GCS_PROJECT             = var.project_id
        GCS_BUCKET              = var.gcs_bucket_name
        BQ_DATASET             = var.bq_audit_dataset
        BQ_TABLE               = var.bq_audit_table
        BQ_GOLD_DATASET        = var.bq_gold_dataset
        CHECKSUM_EXTENSION     = ".sha256"
        MAX_MISSING_FILES_ALLOWED = "0"
        ENVIRONMENT            = var.environment_tag
        PGP_GNUPGHOME          = "/tmp/gnupg"

        # Path to the multi-directory SFTP config YAML — baked into the
        # image by the Dockerfile (COPY manifests/sftp_directories.yaml).
        # Consumed by entrypoint.sh and pipeline/directory_config.py.
        SFTP_DIRECTORIES_YAML = "/opt/airflow/manifests/sftp_directories.yaml"

        # NOTE: SFTP_REMOTE_DIR intentionally NOT set — directories are
        # defined in sftp_directories.yaml, not a single env var.
      }

      pypi_packages  = var.extra_pypi_packages
      scheduler_count = var.scheduler_count
    }

    node_config {
      service_account = google_service_account.composer_worker.email
    }

    workloads_config {

      scheduler {
        cpu          = var.scheduler_cpu
        memory_gb    = var.scheduler_memory_gb
        storage_gb   = var.scheduler_storage_gb
        count        = var.scheduler_count
      }

      web_server {
        cpu        = var.webserver_cpu
        memory_gb  = var.webserver_memory_gb
        storage_gb = var.webserver_storage_gb
      }

      worker {
        cpu        = var.worker_cpu
        memory_gb  = var.worker_memory_gb
        storage_gb = var.worker_storage_gb
        min_count  = var.worker_min_count
        max_count  = var.worker_max_count
      }
    }

    environment_size = var.environment_size

    private_environment_config {
      enable_private_endpoint = var.enable_private_endpoint
    }
  }

  labels = var.labels
}

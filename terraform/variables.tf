variable "project_id" {
  description = "GCP project ID. e.g. insureflow-dev-project, insureflow-prod-project."
  type        = string
}

variable "region" {
  description = "Primary GCP region for all resources."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Logical environment name: dev or prod. Drives ENVIRONMENT env var and resource naming/sizing."
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be 'dev' or 'prod'."
  }
}

variable "gcs_bucket_name" {
  description = "Name of the data lake bucket. Must match GCS_BUCKET consumed by pipeline/config.py."
  type        = string
}

variable "pgp_encrypted_dir_ids" {
  description = "dir_id values from manifests/sftp_directories.yaml that use_pgp: true. Must match pipeline/config.py PGP_ENCRYPTED_DIR_IDS."
  type        = list(string)
  default     = ["inbound"]
}

variable "bq_audit_dataset" {
  type    = string
  default = "pipeline_audit"
}

variable "bq_audit_table" {
  type    = string
  default = "ingestion_runs"
}

variable "bq_gold_dataset" {
  type    = string
  default = "gold_insurance"
}

variable "composer_image_version" {
  type    = string
  default = "composer-2.9.9-airflow-2.8.4"
}

variable "composer_environment_size" {
  description = "ENVIRONMENT_SIZE_SMALL | ENVIRONMENT_SIZE_MEDIUM | ENVIRONMENT_SIZE_LARGE"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
}

variable "worker_min_count" {
  type    = number
  default = 1
}

variable "worker_max_count" {
  type    = number
  default = 3
}

variable "deletion_protection" {
  description = "Applies to BigQuery tables. Set false in dev, true in prod."
  type        = bool
  default     = true
}

variable "bucket_force_destroy" {
  description = "Allow `terraform destroy` to remove a non-empty bucket. Set false in prod."
  type        = bool
  default     = false
}

variable "labels" {
  description = "Common labels applied to all resources."
  type        = map(string)
  default     = {}
}
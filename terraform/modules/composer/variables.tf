
variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "environment_name" {
  description = "Cloud Composer environment name, e.g. insureflow-dev, insureflow-prod."
  type        = string
}

variable "environment_tag" {
  description = "Value of the ENVIRONMENT env var. 'dev' makes entrypoint.sh skip Secret Manager."
  type        = string
}

variable "service_account_id" {
  description = "account_id (not email) for the Composer worker service account."
  type        = string
  default     = "insureflow-composer-worker"
}

variable "composer_image_version" {
  description = "Cloud Composer image version string, e.g. composer-2.9.9-airflow-2.8.4. Keep Airflow minor aligned with requirements.txt's apache-airflow pin."
  type        = string
  default     = "composer-2.9.9-airflow-2.8.4"
}

variable "gcs_bucket_name" {
  description = "Data lake bucket name — passed through as GCS_BUCKET."
  type        = string
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

variable "extra_pypi_packages" {
  description = "Extra PyPI packages to install in the Composer environment (paramiko, python-gnupg, pandera, etc. — keep versions aligned with requirements.txt)."
  type        = map(string)
  default = {
    paramiko          = "==3.4.0"
    python-gnupg      = "==0.5.2"
    pandera           = "==0.19.3"
    pydantic-settings = "==2.2.1"
  }
}

variable "scheduler_count" {
  type    = number
  default = 1
}

variable "scheduler_cpu" {
  type    = number
  default = 2
}

variable "scheduler_memory_gb" {
  type    = number
  default = 4
}

variable "scheduler_storage_gb" {
  type    = number
  default = 5
}

variable "webserver_cpu" {
  type    = number
  default = 1
}

variable "webserver_memory_gb" {
  type    = number
  default = 2
}

variable "webserver_storage_gb" {
  type    = number
  default = 5
}

variable "worker_cpu" {
  type    = number
  default = 2
}

variable "worker_memory_gb" {
  type    = number
  default = 8
}

variable "worker_storage_gb" {
  type    = number
  default = 10
}

variable "worker_min_count" {
  type    = number
  default = 1
}

variable "worker_max_count" {
  type    = number
  default = 3
}

variable "environment_size" {
  description = "ENVIRONMENT_SIZE_SMALL | ENVIRONMENT_SIZE_MEDIUM | ENVIRONMENT_SIZE_LARGE"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
}

variable "enable_private_endpoint" {
  type    = bool
  default = false
}

variable "labels" {
  type    = map(string)
  default = {}
}


variable "project_id" {
  type = string
}

variable "region" {
  description = "BigQuery dataset location, e.g. US, EU, europe-west1."
  type        = string
}

variable "audit_dataset_id" {
  description = "Must match BQ_DATASET in pipeline/config.py."
  type        = string
  default     = "pipeline_audit"
}

variable "audit_table_id" {
  description = "Must match BQ_TABLE in pipeline/config.py."
  type        = string
  default     = "ingestion_runs"
}

variable "gold_dataset_id" {
  description = "Must match BQ_GOLD_DATASET in pipeline/config.py."
  type        = string
  default     = "gold_insurance"
}

variable "composer_service_account_email" {
  description = "Service account used by Cloud Composer workers — granted dataEditor on both datasets and project-level bigquery.jobUser."
  type        = string
}

variable "deletion_protection" {
  description = "Prevent accidental table deletion via terraform destroy. Set true in prod."
  type        = bool
  default     = true
}

variable "labels" {
  type    = map(string)
  default = {}
}

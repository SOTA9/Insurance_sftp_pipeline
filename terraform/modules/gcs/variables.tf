
variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCS bucket location (region or multi-region)"
  type        = string
}

variable "bucket_name" {
  description = "Name of the data lake bucket. Must match GCS_BUCKET env var consumed by pipeline/config.py."
  type        = string
}

variable "composer_service_account_email" {
  description = "Service account email used by Cloud Composer workers — granted objectAdmin on the data lake bucket."
  type        = string
}

variable "force_destroy" {
  description = "Allow Terraform to delete the bucket even if it contains objects. Set false in prod."
  type        = bool
  default     = false
}

variable "enable_versioning" {
  description = "Enable object versioning on the data lake bucket."
  type        = bool
  default     = true
}

variable "bronze_retention_days" {
  description = "Days to retain bronze/ objects before deletion. Silver/Gold are the durable layers."
  type        = number
  default     = 90
}

variable "dead_letter_retention_days" {
  description = "Days to retain dead_letter/ objects before deletion."
  type        = number
  default     = 30
}

variable "labels" {
  description = "Labels applied to the bucket."
  type        = map(string)
  default     = {}
}

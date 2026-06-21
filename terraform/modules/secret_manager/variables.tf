
variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Region for the user-managed secret replica."
  type        = string
}

variable "composer_service_account_email" {
  description = "Service account email used by Cloud Composer workers — granted secretAccessor on every secret."
  type        = string
}

variable "pgp_encrypted_dir_ids" {
  description = <<-EOT
List of dir_id values that use PGP encryption (use_pgp: true in
manifests/sftp_directories.yaml). MUST match
PGP_ENCRYPTED_DIR_IDS in pipeline/config.py.

For each dir_id, creates:
- {dir_id}-pgp-private-key
- {dir_id}-pgp-passphrase.
EOT

  type    = list(string)
  default = ["inbound"]
}

variable "labels" {
  description = "Labels applied to every secret."
  type        = map(string)
  default     = {}
}
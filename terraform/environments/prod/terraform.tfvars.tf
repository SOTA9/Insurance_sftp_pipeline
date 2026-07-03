project_id  = "insureflow-prod-project"
region      = "us-central1"
environment = "prod"

gcs_bucket_name       = "insureflow-datalake-prod"
pgp_encrypted_dir_ids = ["inbound"]

bq_audit_dataset = "pipeline_audit"
bq_audit_table   = "ingestion_runs"
bq_gold_dataset  = "gold_insurance"

composer_image_version    = "composer-2.9.9-airflow-2.8.4"
composer_environment_size = "ENVIRONMENT_SIZE_MEDIUM"

worker_min_count = 1
worker_max_count = 3

# Prod: protect against accidental destroy.
deletion_protection = true
bucket_force_destroy = false

labels = {
  team        = "data-engineering"
  cost_center = "insureflow-prod"
}

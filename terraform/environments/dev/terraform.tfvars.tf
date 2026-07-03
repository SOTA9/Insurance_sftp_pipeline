project_id   = "insureflow-dev-project"
region       = "us-central1"
environment  = "dev"

gcs_bucket_name          = "insureflow-datalake-dev"
pgp_encrypted_dir_ids    = ["inbound"]

bq_audit_dataset = "pipeline_audit"
bq_audit_table   = "ingestion_runs"
bq_gold_dataset  = "gold_insurance"

composer_image_version    = "composer-2.9.9-airflow-2.8.4"
composer_environment_size = "ENVIRONMENT_SIZE_SMALL"

worker_min_count = 1
worker_max_count = 1

# Dev: allow easy teardown / non-protected tables.
deletion_protection = false
bucket_force_destroy = true

labels = {
  team        = "data-engineering"
  cost_center = "insureflow-dev"
}
output "composer_environment_name" {
  value = module.composer.environment_name
}

output "composer_airflow_uri" {
  value = module.composer.airflow_uri
}

output "composer_dag_gcs_prefix" {
  description = "Upload dags/, pipeline/, schemas/, manifests/, keys/public_key.asc here."
  value       = module.composer.dag_gcs_prefix
}

output "composer_service_account_email" {
  value = module.composer.composer_service_account_email
}

output "datalake_bucket_name" {
  value = module.gcs.bucket_name
}

output "secret_ids" {
  value = module.secret_manager.secret_ids
}

output "bq_audit_dataset" {
  value = module.bigquery.audit_dataset_id
}

output "bq_gold_dataset" {
  value = module.bigquery.gold_dataset_id
}

output "artifact_registry_repository_id" {
  description = "Pass to .gitlab-ci.yml as the image repository name: insureflow-docker-<environment>."
  value       = google_artifact_registry_repository.insureflow_docker.repository_id
}
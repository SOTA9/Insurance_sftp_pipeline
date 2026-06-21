
output "environment_name" {
  value = google_composer_environment.insureflow.name
}

output "airflow_uri" {
  value = google_composer_environment.insureflow.config[0].airflow_uri
}

output "dag_gcs_prefix" {
  description = "GCS path to upload dags/, pipeline/, schemas/, manifests/, keys/ into (Composer's own bucket)."
  value       = google_composer_environment.insureflow.config[0].dag_gcs_prefix
}

output "composer_service_account_email" {
  value = google_service_account.composer_worker.email
}

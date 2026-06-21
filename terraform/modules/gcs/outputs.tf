
output "bucket_name" {
  description = "Name of the data lake bucket (use as GCS_BUCKET env var)."
  value       = google_storage_bucket.datalake.name
}

output "bucket_url" {
  description = "gs:// URL of the data lake bucket."
  value       = google_storage_bucket.datalake.url
}

output "bucket_self_link" {
  description = "Self link of the data lake bucket."
  value       = google_storage_bucket.datalake.self_link
}

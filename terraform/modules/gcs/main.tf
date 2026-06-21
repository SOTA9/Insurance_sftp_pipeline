# Data lake bucket. Layout used by the pipeline (see pipeline/*.py):
# bronze/{dir_id}/{entity}/year=/month=/day=/*.csv
# silver/{entity}/year=/month=/day=/*.parquet
# dead_letter/{dir_id}/{entity}/{year}/{month}/{day}/*.csv
# _state/sftp/{dir_id}/processed_files.json
# _state/silver/{entity}/processed_dates.json
# _state/gold/{table_name}/loaded_dates.json

resource "google_storage_bucket" "datalake" {
  name     = var.bucket_name
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy

  versioning {
    enabled = var.enable_versioning
  }

  lifecycle_rule {
    condition {
      age = var.dead_letter_retention_days
    }

    action {
      type = "Delete"
    }

    # Only applies to dead_letter/ objects via matches_prefix below.
  }

  lifecycle_rule {
    condition {
      age            = var.dead_letter_retention_days
      matches_prefix = ["dead_letter/"]
    }

    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age            = var.bronze_retention_days
      matches_prefix = ["bronze/"]
    }

    action {
      type = "Delete"
    }
  }

  labels = var.labels
}

# Composer's own bucket (DAGs, plugins, logs) is created by the Composer
# resource automatically; this module only manages the pipeline data lake.

resource "google_storage_bucket_iam_member" "composer_sa_object_admin" {
  bucket = google_storage_bucket.datalake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.composer_service_account_email}"
}
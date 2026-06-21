# Two datasets:
# 1. pipeline_audit.ingestion_runs — schema MUST match SCHEMA in
# pipeline/audit_logger.py exactly (including dir_id, gcs_prefix).
# 2. gold_insurance — 3 dimension + 3 fact + 3 aggregate tables, written by
# pipeline/transforms/silver_to_gold.py (TBL_DIM_*, TBL_FACT_*, TBL_AGG_*).
# Gold table schemas are produced by pandas-gbq style load jobs with
# schema autodetection from the silver Parquet/DataFrame, so they are
# declared here with autodetect-friendly placeholder schemas. Terraform
# creates the *dataset and empty tables*; the pipeline's first write
# populates/extends the schema via load-job autodetect on top of this.

# Dataset: pipeline_audit

resource "google_bigquery_dataset" "audit" {
  project                        = var.project_id
  dataset_id                     = var.audit_dataset_id
  friendly_name                  = "InsureFlow pipeline audit log"
  description                    = "Per-file audit trail written by pipeline/audit_logger.py after every ingestion run."
  location                       = var.region
  default_table_expiration_ms    = null
  labels                         = var.labels
}

# Schema MUST match pipeline/audit_logger.py::SCHEMA exactly.

resource "google_bigquery_table" "ingestion_runs" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.audit.dataset_id
  table_id            = var.audit_table_id
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "run_id",         type = "STRING",    mode = "REQUIRED" },
    { name = "business_date",  type = "DATE",      mode = "REQUIRED" },
    { name = "gcs_uri",       type = "STRING",    mode = "REQUIRED" },
    { name = "file_type",     type = "STRING",    mode = "NULLABLE" },
    { name = "rows_valid",    type = "INTEGER",   mode = "NULLABLE" },
    { name = "rows_dropped",  type = "INTEGER",   mode = "NULLABLE" },
    { name = "ingested_at",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "dir_id",        type = "STRING",    mode = "NULLABLE" },
    { name = "gcs_prefix",    type = "STRING",    mode = "NULLABLE" },
  ])

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }

  clustering = ["dir_id", "file_type"]
  labels     = var.labels
}

# ── Dataset: gold_insurance ───────────────────────────────────────────────────

resource "google_bigquery_dataset" "gold" {
  project      = var.project_id
  dataset_id   = var.gold_dataset_id
  friendly_name = "InsureFlow gold layer"
  description  = "Dimension, fact, and aggregate tables loaded by pipeline/transforms/silver_to_gold.py."
  location     = var.region
  labels       = var.labels
}

# Dimension tables

resource "google_bigquery_table" "dim_policy" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "dim_policy"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "policy_id", type = "STRING", mode = "REQUIRED" },
  ])

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

resource "google_bigquery_table" "dim_claimant" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "dim_claimant"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "policy_id", type = "STRING", mode = "REQUIRED" },
  ])

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

resource "google_bigquery_table" "dim_reinsurer" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "dim_reinsurer"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "reinsurer_id", type = "STRING", mode = "REQUIRED" },
  ])

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

# Fact tables

resource "google_bigquery_table" "fact_premiums" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "fact_premiums"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "premium_id",     type = "STRING", mode = "REQUIRED" },
    { name = "policy_id",      type = "STRING", mode = "NULLABLE" },
    { name = "ingestion_date", type = "DATE",   mode = "REQUIRED" },
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_date"
  }

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

resource "google_bigquery_table" "fact_claims" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "fact_claims"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "claim_id",       type = "STRING", mode = "REQUIRED" },
    { name = "policy_id",      type = "STRING", mode = "NULLABLE" },
    { name = "ingestion_date", type = "DATE",   mode = "REQUIRED" },
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_date"
  }

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

resource "google_bigquery_table" "fact_reinsurance" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "fact_reinsurance"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "ri_record_id",   type = "STRING", mode = "REQUIRED" },
    { name = "policy_id",      type = "STRING", mode = "NULLABLE" },
    { name = "ingestion_date", type = "DATE",   mode = "REQUIRED" },
  ])

  time_partitioning {
    type  = "DAY"
    field = "ingestion_date"
  }

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

# Aggregate tables

resource "google_bigquery_table" "agg_daily_premium_summary" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_daily_premium_summary"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "business_date", type = "DATE", mode = "REQUIRED" },
  ])

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

resource "google_bigquery_table" "agg_claim_by_status" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_claim_by_status"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "business_date", type = "DATE", mode = "REQUIRED" },
  ])

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

resource "google_bigquery_table" "agg_reinsurer_exposure" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_reinsurer_exposure"
  deletion_protection = var.deletion_protection

  schema = jsonencode([
    { name = "business_date", type = "DATE", mode = "REQUIRED" },
  ])

  labels = var.labels

  lifecycle {
    ignore_changes = [schema]
  }
}

# IAM

resource "google_bigquery_dataset_iam_member" "composer_sa_audit_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.audit.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.composer_service_account_email}"
}

resource "google_bigquery_dataset_iam_member" "composer_sa_gold_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.composer_service_account_email}"
}

resource "google_project_iam_member" "composer_sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.composer_service_account_email}"
}

output "audit_dataset_id" {
  value = google_bigquery_dataset.audit.dataset_id
}

output "gold_dataset_id" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "ingestion_runs_table_id" {
  value = google_bigquery_table.ingestion_runs.table_id
}

output "gold_table_ids" {
  value = [
    google_bigquery_table.dim_policy.table_id,
    google_bigquery_table.dim_claimant.table_id,
    google_bigquery_table.dim_reinsurer.table_id,
    google_bigquery_table.fact_premiums.table_id,
    google_bigquery_table.fact_claims.table_id,
    google_bigquery_table.fact_reinsurance.table_id,
    google_bigquery_table.agg_daily_premium_summary.table_id,
    google_bigquery_table.agg_claim_by_status.table_id,
    google_bigquery_table.agg_reinsurer_exposure.table_id,
  ]
}

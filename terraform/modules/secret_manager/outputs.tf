
output "secret_ids" {
  description = "All secret IDs created by this module (global + per-directory PGP)."
  value       = [for s in google_secret_manager_secret.secret : s.secret_id]
}

output "global_secret_ids" {
  value = local.global_secret_ids
}

output "pgp_secret_ids" {
  value = local.pgp_secret_ids
}

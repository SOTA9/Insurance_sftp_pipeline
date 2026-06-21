# Backend is intentionally left without a hardcoded bucket/prefix. Each
# environment supplies its own backend.hcl via:
#
# terraform init -backend-config=environments/dev/backend.hcl
# terraform init -backend-config=environments/prod/backend.hcl


terraform {
  required_version = ">= 1.7.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }

  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

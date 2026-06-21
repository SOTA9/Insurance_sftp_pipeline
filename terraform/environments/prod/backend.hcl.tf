# terraform/environments/prod/backend.hcl
# Usage: terraform init -backend-config=environments/prod/backend.hcl

bucket = "insureflow-tfstate-prod"
prefix = "insureflow/prod"
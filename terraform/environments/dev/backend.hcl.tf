# terraform/environments/dev/backend.hcl
# Usage: terraform init -backend-config=environments/dev/backend.hcl

bucket = "insureflow-tfstate-dev"
prefix = "insureflow/dev"

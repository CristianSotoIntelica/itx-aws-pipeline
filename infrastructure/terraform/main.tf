# ============================================================
# ITX AWS Pipeline — Terraform Main
# Actualizado: tags corporativos requeridos por Intelica IT
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Backend remoto — activar cuando tengas el nuevo ambiente empresarial
  # backend "s3" {
  #   bucket         = "itx-terraform-state"
  #   key            = "itx-pipeline/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "itx-terraform-locks"
  #   encrypt        = true
  # }
}

# ── Tags corporativos locales ─────────────────────────────────
# Estos tags se aplican automáticamente a TODOS los recursos
# Requeridos por Intelica IT (Hildebrando Nunez)
locals {
  corporate_tags = {
    Environment = var.environment
    Project     = var.tag_project
    ManagedBy   = "Terraform"
    Team        = "Data Engineering"
  }
}

provider "aws" {
  region = var.aws_region

  # default_tags aplica los tags a cada recurso creado por Terraform
  # equivalente a TAGS_IAM / TAGS_LAMBDA / TAGS_GLUE / TAGS_JSON del deploy.sh
  default_tags {
    tags = local.corporate_tags
  }
}

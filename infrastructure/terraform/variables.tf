# ============================================================
# ITX AWS Pipeline — Variables
# Sincronizado con deploy.sh v2
# ============================================================

variable "aws_region" {
  description = "Región AWS donde se despliega la infraestructura"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Ambiente de despliegue (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "El ambiente debe ser: dev, staging o prod."
  }
}

variable "project_prefix" {
  description = "Prefijo para nombrar todos los recursos del proyecto"
  type        = string
  default     = "itx"
}

# ── Tags corporativos — requeridos por Intelica IT ────────────
variable "tag_project" {
  description = "Tag corporativo Project requerido por Intelica IT (Hildebrando Nunez)"
  type        = string
  default     = "datalake-itx"
}

# ── S3 ───────────────────────────────────────────────────────
variable "s3_versioning_enabled" {
  description = "Habilitar versionado en buckets S3"
  type        = bool
  default     = false
}

# ── Lambda ───────────────────────────────────────────────────
variable "lambda_runtime" {
  description = "Runtime de Python para las Lambdas"
  type        = string
  default     = "python3.11"
}

variable "lambda_timeout_default" {
  description = "Timeout para Lambdas simples — router y archive (segundos)"
  type        = number
  default     = 240
}

variable "lambda_timeout_processing" {
  description = "Timeout para Lambdas de procesamiento — transform, extract, clean (segundos)"
  type        = number
  default     = 900
}

variable "lambda_memory_default" {
  description = "Memoria para Lambdas simples — router y archive (MB)"
  type        = number
  default     = 8192
}

# CAMBIO v2: 1024 → 3008 MB
# Razón: archivos interchange requieren más RAM para procesamiento en chunks grandes
variable "lambda_memory_processing" {
  description = "Memoria para Lambdas de procesamiento — transform, extract, clean (MB)"
  type        = number
  default     = 10240
}

# ── Transform Lambda ─────────────────────────────────────────
variable "transform_chunk_size_mb" {
  description = "Tamaño de chunk en MB para itx-transform"
  type        = number
  default     = 128
}

variable "transform_flush_batch_size" {
  description = "Tamaño de batch para flush en itx-transform"
  type        = number
  default     = 1000000
}

# ── Extract / Clean Lambda ───────────────────────────────────
variable "extract_chunk_size" {
  description = "Tamaño de chunk para itx-extract"
  type        = number
  default     = 300000
}

variable "clean_chunk_size" {
  description = "Tamaño de chunk para itx-clean"
  type        = number
  default     = 300000
}

# ── Glue ─────────────────────────────────────────────────────
# CAMBIO v2: 3.0 → 4.0
variable "glue_version" {
  description = "Versión de AWS Glue para ambos jobs"
  type        = string
  default     = "4.0"
}

# itx-calculate: G.1X · 2 workers
variable "glue_calculate_worker_type" {
  description = "Worker type para itx-calculate"
  type        = string
  default     = "G.1X"
}

variable "glue_calculate_workers" {
  description = "Número de workers para itx-calculate"
  type        = number
  default     = 2
}

# itx-interchange: G.2X · 4 workers (CAMBIO v2: Standard·2 → G.2X·4)
variable "glue_interchange_worker_type" {
  description = "Worker type para itx-interchange — más potente por ser job de mayor carga"
  type        = string
  default     = "G.2X"
}

variable "glue_interchange_workers" {
  description = "Número de workers para itx-interchange"
  type        = number
  default     = 4
}

# ── CloudWatch ───────────────────────────────────────────────
variable "log_retention_days" {
  description = "Días de retención de logs en CloudWatch"
  type        = number
  default     = 30
}

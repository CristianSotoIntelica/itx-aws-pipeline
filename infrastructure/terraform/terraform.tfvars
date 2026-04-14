# ============================================================
# ITX AWS Pipeline — Valores de Variables
# Sincronizado con deploy.sh v2
# ============================================================

aws_region     = "us-east-1"
environment    = "dev"
project_prefix = "itx"

# Tags corporativos (Intelica IT - Hildebrando Nunez)
tag_project = "datalake-itx"

# S3
s3_versioning_enabled = false

# Lambda — configuración base
lambda_runtime            = "python3.11"
lambda_timeout_default    = 240
lambda_timeout_processing = 900
lambda_memory_default     = 8192

# CAMBIO v2: 1024 → 3008 MB
lambda_memory_processing  = 10240

# Transform — CAMBIO v2: chunk 64→16, flush 500000→200000
transform_chunk_size_mb    = 128
transform_flush_batch_size = 1000000

# Extract / Clean — sin cambios
extract_chunk_size = 300000
clean_chunk_size   = 300000

# Glue — CAMBIO v2: version 3.0→4.0, worker types diferenciados
glue_version                 = "4.0"
glue_calculate_worker_type   = "G.1X"
glue_calculate_workers       = 2
glue_interchange_worker_type = "G.2X"
glue_interchange_workers     = 4

# CloudWatch
log_retention_days = 60

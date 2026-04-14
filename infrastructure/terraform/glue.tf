# ============================================================
# ITX AWS Pipeline — AWS Glue
# Sincronizado con deploy.sh v2
# Cambios: version 4.0, G.1X/G.2X, args de monitoring
# ============================================================

# ── itx-calculate ────────────────────────────────────────────
# CAMBIO v2: version 3.0 → 4.0 | worker Standard → G.1X | 2 workers
# Nuevos args: enable-metrics, enable-job-insights, continuous-cloudwatch-log
resource "aws_glue_job" "itx_calculate" {
  name         = "itx-calculate"
  role_arn     = aws_iam_role.glue_calculate_role.arn
  glue_version = var.glue_version  # "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.itx_buckets["staging"].bucket}/scripts/calculate.py"
    python_version  = "3"
  }

  default_arguments = {
    # Argumentos base
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.itx_buckets["staging"].bucket}/temp/"

    # Referencias a S3
    "--S3_STAGING"   = "s3://${aws_s3_bucket.itx_buckets["staging"].bucket}"
    "--S3_REFERENCE" = "s3://${aws_s3_bucket.itx_buckets["reference"].bucket}"

    # NUEVO v2: Monitoring y observabilidad
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  # CAMBIO v2: Standard → G.1X · 2 workers
  worker_type       = var.glue_calculate_worker_type  # "G.1X"
  number_of_workers = var.glue_calculate_workers      # 2

  depends_on = [aws_s3_object.glue_calculate_script]
}

# ── itx-interchange ──────────────────────────────────────────
# CAMBIO v2: version 3.0 → 4.0 | Standard·2 → G.2X·4 workers
# Más potente porque procesa todas las reglas de interchange (mayor carga)
# Nuevos args: job-bookmark, enable-job-insights, continuous-cloudwatch-log
resource "aws_glue_job" "itx_interchange" {
  name         = "itx-interchange"
  role_arn     = aws_iam_role.glue_interchange_role.arn
  glue_version = var.glue_version  # "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.itx_buckets["staging"].bucket}/scripts/interchange.py"
    python_version  = "3"
  }

  default_arguments = {
    # Argumentos base
    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.itx_buckets["staging"].bucket}/temp/"

    # Referencias a S3
    "--S3_STAGING"   = "s3://${aws_s3_bucket.itx_buckets["staging"].bucket}"
    "--S3_REFERENCE" = "s3://${aws_s3_bucket.itx_buckets["reference"].bucket}"

    # NUEVO v2: Job bookmark desactivado (reprocesa siempre desde cero)
    "--job-bookmark-option" = "job-bookmark-disable"

    # NUEVO v2: Monitoring y observabilidad
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  # CAMBIO v2: Standard·2 → G.2X·4 (mayor potencia para interchange rules)
  worker_type       = var.glue_interchange_worker_type  # "G.2X"
  number_of_workers = var.glue_interchange_workers      # 4

  depends_on = [aws_s3_object.glue_interchange_script]
}

# ── Glue Databases ────────────────────────────────────────────
resource "aws_glue_catalog_database" "itx_reference" {
  name        = "itx_reference"
  description = "Catálogo de datos de referencia (exchange rates, visa rules, currency, country)"
}

resource "aws_glue_catalog_database" "ebgr_visa_staging" {
  name        = "ebgr_visa_staging"
  description = "Datos procesados del cliente EBGR para consultas Athena"
}

# ── Glue Crawlers ─────────────────────────────────────────────
resource "aws_glue_crawler" "itx_crawler_reference" {
  name          = "itx-crawler-reference"
  role          = aws_iam_role.glue_crawler_reference_role.arn
  database_name = aws_glue_catalog_database.itx_reference.name
  description   = "Cataloga archivos de referencia en itx-reference-dev"

  s3_target {
    path = "s3://${aws_s3_bucket.itx_buckets["reference"].bucket}/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_crawler" "itx_crawler_ebgr_staging" {
  name          = "itx-crawler-ebgr-staging"
  role          = aws_iam_role.glue_crawler_ebgr_role.arn
  database_name = aws_glue_catalog_database.ebgr_visa_staging.name
  description   = "Cataloga datos procesados del cliente EBGR"

  s3_target {
    path = "s3://${aws_s3_bucket.itx_buckets["staging"].bucket}/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

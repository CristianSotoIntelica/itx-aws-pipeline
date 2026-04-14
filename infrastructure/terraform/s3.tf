# ============================================================
# ITX AWS Pipeline — S3 Buckets
# 5 buckets que forman el Data Lake del proyecto
# ============================================================

locals {
  buckets = {
    landing     = "${var.project_prefix}-landing-${var.environment}"
    staging     = "${var.project_prefix}-staging-${var.environment}"
    operational = "${var.project_prefix}-operational-${var.environment}"
    archive     = "${var.project_prefix}-archive-${var.environment}"
    reference   = "${var.project_prefix}-reference-${var.environment}"
  }
}

# ── Crear los 5 buckets ──────────────────────────────────────
resource "aws_s3_bucket" "itx_buckets" {
  for_each = local.buckets
  bucket   = each.value
}

# ── Bloquear acceso público en todos los buckets ─────────────
resource "aws_s3_bucket_public_access_block" "itx_buckets" {
  for_each = local.buckets

  bucket                  = aws_s3_bucket.itx_buckets[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Versionado (configurable por variable) ───────────────────
resource "aws_s3_bucket_versioning" "itx_buckets" {
  for_each = local.buckets

  bucket = aws_s3_bucket.itx_buckets[each.key].id

  versioning_configuration {
    status = var.s3_versioning_enabled ? "Enabled" : "Suspended"
  }
}

# ── Subir scripts de Glue al bucket staging ──────────────────
resource "aws_s3_object" "glue_calculate_script" {
  bucket = aws_s3_bucket.itx_buckets["staging"].id
  key    = "scripts/calculate.py"
  source = "../../../glue/scripts/calculate.py"
  etag   = filemd5("../../../glue/scripts/calculate.py")
}

resource "aws_s3_object" "glue_interchange_script" {
  bucket = aws_s3_bucket.itx_buckets["staging"].id
  key    = "scripts/interchange.py"
  source = "../../../glue/scripts/interchange.py"
  etag   = filemd5("../../../glue/scripts/interchange.py")
}

# ── S3 Event Notification: landing → itx-router ──────────────
resource "aws_s3_bucket_notification" "landing_trigger" {
  bucket = aws_s3_bucket.itx_buckets["landing"].id

  lambda_function {
    id                  = "TriggerRouterOnUpload"
    lambda_function_arn = aws_lambda_function.itx_router.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.s3_trigger_router]
}

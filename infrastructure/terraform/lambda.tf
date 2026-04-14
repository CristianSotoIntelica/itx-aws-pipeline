# ============================================================
# ITX AWS Pipeline — Lambda Functions
# Sincronizado con deploy.sh v2
# Cambios: memoria 3008MB, transform chunk/flush actualizados
# ============================================================

# ── Lambda Layer: itx-pandas-pyarrow ─────────────────────────
resource "aws_lambda_layer_version" "pandas_pyarrow" {
  layer_name          = "itx-pandas-pyarrow"
  description         = "pandas y pyarrow para procesamiento ITX"
  filename            = "../../../layers/itx-pandas-pyarrow/layer.zip"
  source_code_hash    = filebase64sha256("../../../layers/itx-pandas-pyarrow/layer.zip")
  compatible_runtimes = ["python3.11"]
}

# ── Empaquetar código de cada Lambda ─────────────────────────
data "archive_file" "lambda_zips" {
  for_each    = local.lambda_configs
  type        = "zip"
  source_dir  = "../../../lambdas/${each.key}/src"
  output_path = "/tmp/${each.key}.zip"
}

# ── Configuración de cada Lambda ─────────────────────────────
# NOTAS de cambios v2:
locals {
  lambda_configs = {
    "itx-router" = {
      role_key  = "router"
      handler   = "handler.lambda_handler"
      timeout   = var.lambda_timeout_default   
      memory    = var.lambda_memory_default     
      use_layer = false
      env_vars = {
        STEP_FUNCTION_ARN           = aws_sfn_state_machine.itx_main_orchestrator.arn
        DYNAMODB_TABLE_FILE_CONTROL = aws_dynamodb_table.file_control.name
        DYNAMODB_TABLE_FILE_PATTERN = aws_dynamodb_table.file_pattern.name
        S3_BUCKET_LANDING           = aws_s3_bucket.itx_buckets["landing"].bucket
      }
    }

    "itx-transform" = {
      role_key  = "transform"
      handler   = "handler.lambda_handler"
      timeout   = var.lambda_timeout_processing  
      memory    = var.lambda_memory_processing  
      use_layer = true
      env_vars = {
        S3_BUCKET_LANDING  = aws_s3_bucket.itx_buckets["landing"].bucket
        S3_BUCKET_STAGING  = aws_s3_bucket.itx_buckets["staging"].bucket
        CHUNK_SIZE_MB      = tostring(var.transform_chunk_size_mb)    
        FLUSH_BATCH_SIZE   = tostring(var.transform_flush_batch_size) 
      }
    }

    "itx-extract" = {
      role_key  = "extract"
      handler   = "handler.lambda_handler"
      timeout   = var.lambda_timeout_processing 
      memory    = var.lambda_memory_processing  
      use_layer = true
      env_vars = {
        S3_BUCKET_STAGING         = aws_s3_bucket.itx_buckets["staging"].bucket
        DYNAMODB_FIELD_DEFINITION = aws_dynamodb_table.visa_fields.name
        EXTRACT_CHUNK_SIZE        = tostring(var.extract_chunk_size) 
      }
    }

    "itx-clean" = {
      role_key  = "clean"
      handler   = "handler.lambda_handler"
      timeout   = var.lambda_timeout_processing  
      memory    = var.lambda_memory_processing   
      use_layer = true
      env_vars = {
        S3_BUCKET_STAGING         = aws_s3_bucket.itx_buckets["staging"].bucket
        DYNAMODB_FIELD_DEFINITION = aws_dynamodb_table.visa_fields.name
        CLEAN_CHUNK_SIZE          = tostring(var.clean_chunk_size)   
      }
    }

    "itx-store" = {
      role_key  = "store"
      handler   = "handler.lambda_handler"
      timeout   = var.lambda_timeout_processing  
      memory    = var.lambda_memory_processing   
      use_layer = true
      env_vars = {
        S3_BUCKET_STAGING           = aws_s3_bucket.itx_buckets["staging"].bucket
        S3_BUCKET_OPERATIONAL       = aws_s3_bucket.itx_buckets["operational"].bucket
        DYNAMODB_TABLE_FILE_CONTROL = aws_dynamodb_table.file_control.name
      }
    }

    "itx-archive-file" = {
      role_key  = "archive"
      handler   = "handler.lambda_handler"
      timeout   = var.lambda_timeout_default   
      memory    = var.lambda_memory_default      
      use_layer = false
      env_vars = {
        S3_BUCKET_LANDING = aws_s3_bucket.itx_buckets["landing"].bucket
        S3_BUCKET_ARCHIVE = aws_s3_bucket.itx_buckets["archive"].bucket
      }
    }
  }
}

# ── Crear todas las Lambdas ───────────────────────────────────
resource "aws_lambda_function" "itx_lambdas" {
  for_each = local.lambda_configs

  function_name    = each.key
  role             = aws_iam_role.lambda_roles[each.value.role_key].arn
  handler          = each.value.handler
  runtime          = var.lambda_runtime
  timeout          = each.value.timeout
  memory_size      = each.value.memory
  filename         = data.archive_file.lambda_zips[each.key].output_path
  source_code_hash = data.archive_file.lambda_zips[each.key].output_base64sha256

  layers = each.value.use_layer ? [aws_lambda_layer_version.pandas_pyarrow.arn] : []

  environment {
    variables = each.value.env_vars
  }

  # Los tags corporativos se aplican via default_tags en main.tf
  # No es necesario declararlos aquí — Terraform los hereda automáticamente

  depends_on = [
    aws_iam_role_policy_attachment.lambda_base_attachment,
    aws_cloudwatch_log_group.lambda_logs,
  ]
}

# ── Permiso para que S3 invoque itx-router ────────────────────
resource "aws_lambda_permission" "s3_trigger_router" {
  statement_id  = "s3-trigger"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.itx_lambdas["itx-router"].function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.itx_buckets["landing"].arn
}

# ── CloudWatch Log Groups con retención configurada ──────────
resource "aws_cloudwatch_log_group" "lambda_logs" {
  for_each          = local.lambda_configs
  name              = "/aws/lambda/${each.key}"
  retention_in_days = var.log_retention_days
}

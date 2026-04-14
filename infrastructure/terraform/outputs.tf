# ============================================================
# ITX AWS Pipeline — Outputs
# Valores útiles que se muestran al terminar el terraform apply
# ============================================================

output "s3_buckets" {
  description = "ARNs de los buckets S3 del proyecto"
  value = {
    for key, bucket in aws_s3_bucket.itx_buckets :
    key => bucket.bucket
  }
}

output "lambda_arns" {
  description = "ARNs de todas las funciones Lambda"
  value = {
    for key, fn in aws_lambda_function.itx_lambdas :
    key => fn.arn
  }
}

output "lambda_layer_arn" {
  description = "ARN del Lambda Layer itx-pandas-pyarrow"
  value       = aws_lambda_layer_version.pandas_pyarrow.arn
}

output "step_functions_arn" {
  description = "ARN del orquestador Step Functions"
  value       = aws_sfn_state_machine.itx_main_orchestrator.arn
}

output "dynamodb_tables" {
  description = "Nombres de las tablas DynamoDB"
  value = {
    file_control = aws_dynamodb_table.file_control.name
    file_pattern = aws_dynamodb_table.file_pattern.name
    visa_fields  = aws_dynamodb_table.visa_fields.name
    client       = aws_dynamodb_table.client.name
  }
}

output "glue_jobs" {
  description = "Nombres de los Glue Jobs"
  value = {
    calculate   = aws_glue_job.itx_calculate.name
    interchange = aws_glue_job.itx_interchange.name
  }
}

output "glue_databases" {
  description = "Databases del catálogo Glue"
  value = {
    reference     = aws_glue_catalog_database.itx_reference.name
    ebgr_staging  = aws_glue_catalog_database.ebgr_visa_staging.name
  }
}

output "environment" {
  description = "Ambiente desplegado"
  value       = var.environment
}

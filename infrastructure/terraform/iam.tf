# ============================================================
# ITX AWS Pipeline — IAM Roles y Políticas
# Permisos para cada componente del pipeline
# ============================================================

# ── Trust Policies (quién puede asumir cada rol) ─────────────
data "aws_iam_policy_document" "lambda_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "stepfunctions_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

# ── Política base para todas las Lambdas ─────────────────────
data "aws_iam_policy_document" "lambda_base_policy" {
  # Logs en CloudWatch
  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }

  # Acceso a S3 (todos los buckets ITX)
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:CopyObject"]
    resources = [
      for bucket in aws_s3_bucket.itx_buckets : "${bucket.arn}/*"
    ]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [for bucket in aws_s3_bucket.itx_buckets : bucket.arn]
  }

  # Acceso a DynamoDB
  statement {
    effect  = "Allow"
    actions = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Scan", "dynamodb:Query"]
    resources = [
      aws_dynamodb_table.file_control.arn,
      aws_dynamodb_table.file_pattern.arn,
      aws_dynamodb_table.visa_fields.arn,
      aws_dynamodb_table.client.arn,
    ]
  }
}

resource "aws_iam_policy" "lambda_base" {
  name   = "${var.project_prefix}-lambda-base-policy"
  policy = data.aws_iam_policy_document.lambda_base_policy.json
}

# ── Política adicional para router (Step Functions) ───────────
data "aws_iam_policy_document" "router_extra_policy" {
  statement {
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.itx_main_orchestrator.arn]
  }
}

resource "aws_iam_policy" "router_extra" {
  name   = "${var.project_prefix}-lambda-router-extra-policy"
  policy = data.aws_iam_policy_document.router_extra_policy.json
}

# ── Roles de Lambda ───────────────────────────────────────────
locals {
  lambda_roles = {
    router    = "itx-lambda-router-role"
    transform = "itx-lambda-transform-role"
    extract   = "itx-lambda-extract-role"
    clean     = "itx-lambda-clean-role"
    store     = "itx-lambda-store-role"
    archive   = "itx-lambda-archive-role"
  }
}

resource "aws_iam_role" "lambda_roles" {
  for_each           = local.lambda_roles
  name               = each.value
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

# Adjuntar política base a todos los roles Lambda
resource "aws_iam_role_policy_attachment" "lambda_base_attachment" {
  for_each   = local.lambda_roles
  role       = aws_iam_role.lambda_roles[each.key].name
  policy_arn = aws_iam_policy.lambda_base.arn
}

# Política extra solo para router
resource "aws_iam_role_policy_attachment" "router_extra_attachment" {
  role       = aws_iam_role.lambda_roles["router"].name
  policy_arn = aws_iam_policy.router_extra.arn
}

# ── Rol de Step Functions ─────────────────────────────────────
resource "aws_iam_role" "stepfunctions_role" {
  name               = "itx-stepfunctions-role"
  assume_role_policy = data.aws_iam_policy_document.stepfunctions_trust.json
}

data "aws_iam_policy_document" "stepfunctions_policy" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [for fn in aws_lambda_function.itx_lambdas : fn.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["glue:StartJobRun", "glue:GetJobRun"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "stepfunctions_policy" {
  name   = "itx-stepfunctions-inline-policy"
  role   = aws_iam_role.stepfunctions_role.id
  policy = data.aws_iam_policy_document.stepfunctions_policy.json
}

# ── Roles de Glue ─────────────────────────────────────────────
resource "aws_iam_role" "glue_calculate_role" {
  name               = "itx-glue-calculate-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

resource "aws_iam_role" "glue_interchange_role" {
  name               = "itx-glue-interchange-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

resource "aws_iam_role" "glue_crawler_reference_role" {
  name               = "itx-glue-crawler-reference-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

resource "aws_iam_role" "glue_crawler_ebgr_role" {
  name               = "itx-glue-crawler-ebgr-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  for_each = {
    calculate  = aws_iam_role.glue_calculate_role.name
    interchange = aws_iam_role.glue_interchange_role.name
    crawler_ref = aws_iam_role.glue_crawler_reference_role.name
    crawler_ebgr = aws_iam_role.glue_crawler_ebgr_role.name
  }
  role       = each.value
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3_policy" {
  for_each = {
    calculate  = aws_iam_role.glue_calculate_role.name
    interchange = aws_iam_role.glue_interchange_role.name
    crawler_ref = aws_iam_role.glue_crawler_reference_role.name
    crawler_ebgr = aws_iam_role.glue_crawler_ebgr_role.name
  }
  role       = each.value
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

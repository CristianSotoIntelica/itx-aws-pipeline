# ============================================================
# ITX AWS Pipeline — DynamoDB Tables
# 4 tablas de control y configuración del pipeline
# ============================================================

# ── itx-file-control ─────────────────────────────────────────
# Registra el estado de cada archivo procesado por el pipeline
resource "aws_dynamodb_table" "file_control" {
  name         = "itx-file-control"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "file_id"

  attribute {
    name = "file_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = false
  }
}

# ── itx-file-pattern ─────────────────────────────────────────
# Patrones de reconocimiento de archivos por tipo y cliente
# Usado por itx-router para identificar BASEII, SMS, VSS
resource "aws_dynamodb_table" "file_pattern" {
  name         = "itx-file-pattern"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pattern_id"

  attribute {
    name = "pattern_id"
    type = "S"
  }
}

# ── itx-visa-fields ──────────────────────────────────────────
# Definición de campos Visa por tipo de archivo (430 items)
# Usado por itx-extract e itx-clean para mapear y validar campos
resource "aws_dynamodb_table" "visa_fields" {
  name         = "itx-visa-fields"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "field_id"

  attribute {
    name = "field_id"
    type = "S"
  }
}

# ── itx-client ───────────────────────────────────────────────
# Catálogo de clientes del sistema (EBGR, NXGR, BTRO, SBSA, NGGR)
resource "aws_dynamodb_table" "client" {
  name         = "itx-client"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "client_id"

  attribute {
    name = "client_id"
    type = "S"
  }
}

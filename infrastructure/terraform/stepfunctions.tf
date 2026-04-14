# ============================================================
# ITX AWS Pipeline — Step Functions
# Orquestador principal del pipeline
# ============================================================

resource "aws_sfn_state_machine" "itx_main_orchestrator" {
  name     = "itx-main-orchestrator"
  role_arn = aws_iam_role.stepfunctions_role.arn

  # Lee el ASL directamente del archivo exportado del proyecto
  definition = file("../../../step-functions/asl.json")

  logging_configuration {
    level                  = "ERROR"
    include_execution_data = false
  }

  depends_on = [
    aws_iam_role_policy.stepfunctions_policy,
    aws_lambda_function.itx_lambdas,
  ]
}

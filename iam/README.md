# IAM Roles

## Roles actuales

itx-lambda-router-role    -> itx-router, itx-extract (temporal)
itx-lambda-transform-role -> itx-transform
itx-lambda-clean-role     -> itx-clean
itx-lambda-store-role     -> itx-store
itx-lambda-archive-role   -> itx-archive-file
itx-stepfunctions-role    -> itx-main-orchestrator
itx-glue-calculate-role   -> itx-calculate
itx-glue-interchange-role -> itx-interchange
itx-glue-crawler-reference-role -> crawler_itx_reference

## Pendientes en nuevo ambiente
- itx-lambda-extract-role: rol propio para itx-extract
- itx-glue-crawler-ebgr-role: rol para crawler_ebgr_visa_staging

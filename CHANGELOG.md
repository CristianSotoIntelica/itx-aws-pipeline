# CHANGELOG - ITX AWS Pipeline

## [1.0.0] - 2026-04-08
Primera version estable - Migracion cuenta prueba a empresa

### Implementado
- Lambda: itx-router
- Lambda: itx-transform (BASEII + SMS + VSS unificado)
- Lambda: itx-extract
- Lambda: itx-clean
- Lambda: itx-archive-file
- Step Functions: itx-main-orchestrator
- Glue Job: itx-calculate
- Glue Job: itx-interchange
- Glue Crawler: crawler_itx_reference
- Glue Crawler: crawler_ebgr_visa_staging
- Glue Database: itx_reference
- Glue Database: ebgr_visa_staging
- DynamoDB: itx-file-control, itx-file-pattern, itx-visa-fields, itx-client
- S3: landing, staging, operational, archive, reference
- Lambda Layer: itx-pandas-pyarrow v1
- S3 Event: itx-landing -> itx-router
- IAM: 9 roles configurados

### Pendiente - Implementar en nuevo ambiente
- itx-store: generacion de Parquets finales hacia itx-operational
- itx-lambda-extract-role: rol IAM propio para itx-extract
- itx-glue-crawler-ebgr-role: rol IAM propio para crawler EBGR
- Renombrar crawlers y databases con prefijo itx- consistente
- CloudWatch: configurar retencion en log groups
- Glue scripts: mover de itx-staging-dev a itx-reference-dev/scripts/
- Testing end-to-end en nuevo ambiente

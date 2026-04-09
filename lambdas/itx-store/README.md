# itx-store - PENDIENTE DE IMPLEMENTACION

## Descripcion
Genera los archivos Parquet finales del proceso de interchange
y los deposita en el bucket itx-operational-dev.

## Rol en el pipeline
itx-clean -> [Step Functions] -> itx-store -> itx-operational-dev

## Inputs esperados
- Datos limpios y validados desde itx-staging-dev
- Metadata de control desde itx-file-control (DynamoDB)

## Output esperado
- Archivos .parquet en:
  s3://itx-operational-{env}/{client}/{year}/{month}/

## Variables de entorno requeridas
S3_BUCKET_STAGING           = itx-staging-{env}
S3_BUCKET_OPERATIONAL       = itx-operational-{env}
DYNAMODB_TABLE_FILE_CONTROL = itx-file-control

## Estado
PENDIENTE - Implementar en ambiente empresarial

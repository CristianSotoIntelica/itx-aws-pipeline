# itx-router

## Descripcion
Punto de entrada del pipeline. Se activa cuando un archivo
es depositado en itx-landing-dev. Detecta el tipo de archivo
(BASEII, SMS, VSS) y dispara el flujo en Step Functions.

## Trigger
S3 Event: s3:ObjectCreated:* en itx-landing-dev

## Responsabilidades
1. Detectar tipo de archivo via itx-file-pattern (DynamoDB)
2. Registrar entrada en itx-file-control (DynamoDB)
3. Disparar itx-main-orchestrator (Step Functions)

## Variables de entorno
STEP_FUNCTION_ARN           = arn:aws:states:...:itx-main-orchestrator
DYNAMODB_TABLE_FILE_CONTROL = itx-file-control
DYNAMODB_TABLE_FILE_PATTERN = itx-file-pattern
S3_BUCKET_LANDING           = itx-landing-{env}

## IAM Role
itx-lambda-router-role

## Estado
Implementado y en produccion

# itx-extract

## Descripcion
Extrae y mapea los campos del Parquet transformado segun
definiciones configuradas en DynamoDB (itx-visa-fields).

## Responsabilidades
1. Leer Parquet desde itx-staging-dev
2. Consultar definicion de campos en itx-visa-fields
3. Extraer y mapear campos relevantes
4. Escribir resultado en itx-staging-dev/extracted/

## Variables de entorno
S3_BUCKET_STAGING         = itx-staging-{env}
DYNAMODB_FIELD_DEFINITION = itx-visa-fields
EXTRACT_CHUNK_SIZE        = 100000

## Layer
itx-pandas-pyarrow v1

## IAM Role
itx-lambda-router-role
PENDIENTE: crear itx-lambda-extract-role propio en nuevo ambiente

## Estado
Implementado | Pendiente rol IAM propio

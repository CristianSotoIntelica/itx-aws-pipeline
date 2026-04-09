# itx-clean

## Descripcion
Limpia y valida los datos extraidos. Aplica reglas de calidad
y elimina registros invalidos o duplicados.

## Responsabilidades
1. Leer datos extraidos desde itx-staging-dev
2. Validar campos contra definicion en itx-visa-fields
3. Aplicar reglas de limpieza
4. Escribir datos limpios en itx-staging-dev/clean/

## Variables de entorno
S3_BUCKET_STAGING         = itx-staging-{env}
DYNAMODB_FIELD_DEFINITION = itx-visa-fields
CLEAN_CHUNK_SIZE          = 100000

## Layer
itx-pandas-pyarrow v1

## IAM Role
itx-lambda-clean-role

## Estado
Implementado y en produccion

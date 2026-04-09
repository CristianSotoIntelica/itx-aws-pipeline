# itx-transform

## Descripcion
Transforma archivos raw (BASEII, SMS, VSS) de formato EBCDIC
a Parquet optimizado. Unifica los 3 tipos en un schema comun.

## Responsabilidades
1. Leer archivo raw desde itx-landing-dev
2. Decodificar EBCDIC (cp500) a UTF-8
3. Parsear registros segun tipo de archivo
4. Escribir Parquet en itx-staging-dev

## Variables de entorno
S3_BUCKET_LANDING  = itx-landing-{env}
S3_BUCKET_STAGING  = itx-staging-{env}
CHUNK_SIZE_MB      = 64
FLUSH_BATCH_SIZE   = 500000

## Layer
itx-pandas-pyarrow v1

## IAM Role
itx-lambda-transform-role

## Estado
Implementado y en produccion

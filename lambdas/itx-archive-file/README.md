# itx-archive-file

## Descripcion
Mueve el archivo original procesado desde itx-landing-dev
hacia itx-archive-dev como ultimo paso del pipeline.

## Responsabilidades
1. Copiar archivo original a itx-archive-dev
2. Eliminar archivo de itx-landing-dev
3. Actualizar estado en itx-file-control

## Variables de entorno
S3_BUCKET_LANDING = itx-landing-{env}
S3_BUCKET_ARCHIVE = itx-archive-{env}

## IAM Role
itx-lambda-archive-role

## Estado
Implementado y en produccion

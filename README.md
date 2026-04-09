# ITX AWS Pipeline

Pipeline serverless para procesamiento de archivos de interchange
Visa/Mastercard. Transforma archivos raw (BASEII, SMS, VSS) en
datos estructurados en formato Parquet.

## Arquitectura

S3 Landing --[S3 Event]--> itx-router
                               |
               [Step Functions: itx-main-orchestrator]
                               |
                        itx-transform       EBCDIC -> Parquet
                               |
                        itx-extract         Mapeo de campos
                               |
                        itx-clean           Validacion y limpieza
                               |
                        itx-store (*)       Parquets finales
                               |
                        itx-archive-file    Archiva original

(*) Pendiente de implementacion

## Stack

| Servicio       | Uso                                      |
|----------------|------------------------------------------|
| Lambda         | Procesamiento por etapas                 |
| Step Functions | Orquestacion del flujo                   |
| S3             | Data lake (5 buckets)                    |
| Glue           | Calculo de interchange + catalogo        |
| Athena         | Consultas sobre datos procesados         |
| DynamoDB       | Control de archivos y configuracion      |
| CloudWatch     | Logs y monitoreo                         |

## Estructura del Repositorio

itx-aws-pipeline/
├── lambdas/
│   ├── itx-router/
│   ├── itx-transform/
│   ├── itx-extract/
│   ├── itx-clean/
│   ├── itx-store/         <- pendiente
│   └── itx-archive-file/
├── step-functions/
├── glue/scripts/
├── dynamodb/schemas/
├── iam/roles/
├── s3/configs/
├── layers/itx-pandas-pyarrow/
├── athena/
├── infrastructure/
│   └── deploy.sh
├── .env.example
├── .gitignore
├── CHANGELOG.md
└── README.md

## Deploy en nuevo ambiente

git clone https://github.com/<org>/itx-aws-pipeline.git
cd itx-aws-pipeline
cp .env.example .env
# Editar .env con valores del nuevo ambiente
chmod +x infrastructure/deploy.sh
./infrastructure/deploy.sh

## Estado
Pipeline funcional en cuenta de prueba.
En proceso de migracion a ambiente empresarial.
Ver CHANGELOG.md para detalle de pendientes.

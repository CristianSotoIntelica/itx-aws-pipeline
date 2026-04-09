# S3 - Arquitectura de Buckets

## itx-landing-dev
Recepcion de archivos raw (BASEII, SMS, VSS).
Trigger: s3:ObjectCreated:* hacia itx-router.

## itx-staging-dev
Procesamiento intermedio en Parquet por etapas.
Contiene scripts/ con los Glue Jobs.

## itx-operational-dev
Parquets finales listos para consumo.
Se populara cuando itx-store este implementado.

## itx-archive-dev
Archivos originales post-procesamiento.
Destino final de itx-archive-file.

## itx-reference-dev
Archivos de referencia estaticos:
- exchange_rates
- visa_ardef
- visa_rules
- currency
- country

## Nota
En nuevo ambiente reemplazar sufijo -dev segun ambiente.

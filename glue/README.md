# Glue Jobs y Crawlers

## Jobs

### itx-calculate
Calcula el interchange fee por transaccion segun reglas
Visa/Mastercard. Lee desde itx-staging-dev.
- Script: scripts/calculate.py
- Role: itx-glue-calculate-role

### itx-interchange
Genera el archivo de interchange final consolidado.
- Script: scripts/interchange.py
- Role: itx-glue-interchange-role

## Crawlers

### crawler_itx_reference
Cataloga archivos de referencia en itx-reference-dev.
- Database: itx_reference
- Role: itx-glue-crawler-reference-role
- Renombrar en nuevo ambiente: itx-crawler-reference

### crawler_ebgr_visa_staging
Cataloga datos procesados del cliente EBGR para Athena.
- Database: ebgr_visa_staging
- Renombrar en nuevo ambiente: itx-crawler-ebgr-staging
- Crear rol propio: itx-glue-crawler-ebgr-role

## Nota sobre scripts
Scripts actualmente en itx-staging-dev/scripts/.
En nuevo ambiente mover a itx-reference-dev/scripts/.

# DynamoDB - Tablas del Proyecto

## itx-file-control
Control de archivos procesados por el pipeline.
- Items: 55
- Clave PK: file_id

## itx-file-pattern
Patrones de reconocimiento de archivos por tipo y cliente.
Usado por itx-router para identificar el tipo de archivo.
- Items: 5
- Clave PK: pattern_id

## itx-visa-fields
Definicion de campos Visa por tipo de archivo.
Usado por itx-extract e itx-clean.
- Items: 430
- Clave PK: field_id

## itx-client
Catalogo de clientes del sistema.
- Items: 4
- Clave PK: client_id

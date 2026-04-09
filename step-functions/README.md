# Step Functions - itx-main-orchestrator

## Flujo
itx-router (trigger)
  -> itx-transform
    -> itx-extract
      -> itx-clean
        -> itx-store (pendiente)
          -> itx-archive-file

## Archivos
- asl.json: Amazon States Language (definicion del flujo)
- full-config.json: Configuracion completa exportada de AWS

## IAM Role
itx-stepfunctions-role

"""
Lambda: Archive File
====================
Mueve archivo procesado de Landing a Archive bucket.

Referencia: file_receiver.py líneas 383-388
    fs.move_file(
        source_path=filepath,
        target_layer=FileStorage.Layer.OPERATIONAL,
        client_id=client_id,
        target_subdir="originals"
    )

Estructura en Archive (mismo particionamiento que Staging):
    s3://itx-archive-bucket/{client}/{brand}/{file_type}/{date}/{filename}
"""

import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

LANDING_BUCKET = os.environ['S3_BUCKET_LANDING']
ARCHIVE_BUCKET = os.environ['S3_BUCKET_ARCHIVE']


def lambda_handler(event, context):
    """
    Mueve archivo de Landing a Archive y lo elimina de Landing.
    
    Input (desde Step Functions):
    {
        "bucket_landing": "itx-landing-bucket",
        "s3_key_landing": "EBGR/VS.EBGR.TC00.2025015.001.txt",
        "client_id": "EBGR",
        "brand": "VISA",
        "file_type": "IN",
        "file_date": "2025-01-15",
        "content_hash": "A1B2C3D4..."
    }
    """
    logger.info(f"Event: {json.dumps(event)}")
    
    try:
        # Extraer parámetros
        bucket_landing = event.get('bucket_landing', LANDING_BUCKET)
        s3_key_landing = event['s3_key_landing']
        client_id = event['client_id']
        brand = event['brand']
        file_type = event['file_type']  # 'IN' o 'OUT'
        file_date = event['file_date']
        
        # Extraer nombre del archivo del key
        filename = s3_key_landing.split('/')[-1]
        
        # Construir key en Archive (mismo particionamiento que Staging)
        # Estructura: {client}/{brand}/{file_type}/{date}/{filename}
        archive_key = f"{client_id}/{brand}/{file_type}/{file_date}/{filename}"
        
        logger.info(f"Archivando: s3://{bucket_landing}/{s3_key_landing}")
        logger.info(f"Destino: s3://{ARCHIVE_BUCKET}/{archive_key}")
        
        # 1. Copiar a Archive
        s3.copy_object(
            CopySource={'Bucket': bucket_landing, 'Key': s3_key_landing},
            Bucket=ARCHIVE_BUCKET,
            Key=archive_key,
            StorageClass='STANDARD'  # Lifecycle lo moverá a Glacier después
        )
        
        logger.info(f"✅ Copiado a Archive")
        
        # 2. Eliminar de Landing (mantener Landing limpio)
        s3.delete_object(
            Bucket=bucket_landing,
            Key=s3_key_landing
        )
        
        logger.info(f"🗑️ Eliminado de Landing")
        
        return {
            'status': 'ARCHIVED',
            'source': f"s3://{bucket_landing}/{s3_key_landing}",
            'destination': f"s3://{ARCHIVE_BUCKET}/{archive_key}",
            'deleted_from_landing': True
        }
        
    except Exception as e:
        logger.error(f"Error archivando: {e}", exc_info=True)
        # No fallar el pipeline por error de archivado
        # El archivo quedará en Landing y se puede archivar manualmente
        return {
            'status': 'ARCHIVE_FAILED',
            'error': str(e),
            'deleted_from_landing': False
        }

"""
Lambda Unzip - itx-unzip
=========================
Descomprime archivos ZIP que llegan al landing y sube al landing
solo los archivos que hacen match con los patrones de itx-file-pattern.

Flujo:
  1. Recibe el S3 key del ZIP y la file_date extraída por el router
  2. Descarga el ZIP en streaming a /tmp (chunks de 8MB)
  3. Inspecciona los archivos internos sin extraer (solo lee el índice)
  4. Filtra por patrones de DynamoDB — mismos que usa el router
  5. Extrae y sube al landing solo los archivos que hacen match
  6. Archiva el ZIP original → operational/originals/zip/{year}/{month}/
  7. Elimina el ZIP del landing
  8. Los archivos subidos al landing disparan el router via S3 Event
     automáticamente → paralelismo gratis sin configuración adicional

Formatos de ZIP recibidos:
  MAST260416.zip          → Mastercard, YYMMDD en nombre
  VISA260416.zip          → Visa, YYMMDD en nombre
  20260416visaout.zip     → Visa OUT, YYYYMMDD en nombre
  20260416visain.zip      → Visa IN, YYYYMMDD en nombre
  20260416mcin.zip        → Mastercard IN, YYYYMMDD en nombre

Variables de entorno:
  S3_BUCKET_LANDING           : bucket de destino para archivos extraídos
  S3_BUCKET_OPERATIONAL       : bucket para archivar el ZIP original
  DYNAMODB_TABLE_FILE_PATTERN : tabla de patrones (default: itx-file-pattern)
  EXTRACT_CHUNK_SIZE_MB       : chunk para descargar el ZIP (default: 8MB)
"""

import os
import re
import zipfile
import logging
import boto3
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

LANDING_BUCKET      = os.environ.get('S3_BUCKET_LANDING')
OPERATIONAL_BUCKET  = os.environ.get('S3_BUCKET_OPERATIONAL')
FILE_PATTERN_TABLE  = os.environ.get('DYNAMODB_TABLE_FILE_PATTERN', 'itx-file-pattern')
EXTRACT_CHUNK_BYTES = int(os.environ.get('EXTRACT_CHUNK_SIZE_MB', '8')) * 1024 * 1024

MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100MB


# =============================================================================
# PATRONES DESDE DYNAMODB
# Misma lógica que el router — garantiza clasificación consistente
# =============================================================================

def _load_patterns(customer_code: str) -> List[Dict]:
    """
    Carga patrones activos de DynamoDB para el cliente.
    Misma lógica que el router para garantizar consistencia.
    """
    try:
        table    = dynamodb.Table(FILE_PATTERN_TABLE)
        response = table.scan(
            FilterExpression='is_active = :active',
            ExpressionAttributeValues={':active': 1}
        )
        items = response.get('Items', [])

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='is_active = :active',
                ExpressionAttributeValues={':active': 1},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        if not items:
            logger.warning("No active patterns in DynamoDB")
            return []

        items.sort(key=lambda x: int(x.get('priority', 999)))
        items = [p for p in items if p.get('customer_code') in [customer_code, 'ALL']]

        logger.info(f"Loaded {len(items)} patterns for '{customer_code}'")
        return items

    except Exception as e:
        logger.error(f"Error loading patterns: {str(e)}")
        return []


def _matches_pattern(filename: str, patterns: List[Dict]) -> Optional[Dict]:
    """Retorna la clasificación del primer patrón que hace match, o None."""
    for patron in patterns:
        regex = patron.get('file_format', '')
        if not regex:
            continue
        try:
            if re.search(regex, filename, re.IGNORECASE):
                return {
                    'brand':         patron.get('brand', 'UNKNOWN'),
                    'direction':     patron.get('direction', 'UNKNOWN'),
                    'pattern_id':    patron.get('pattern_id'),
                    'customer_code': patron.get('customer_code'),
                }
        except re.error:
            continue
    return None


# =============================================================================
# DESCARGA DEL ZIP A /TMP EN STREAMING
# =============================================================================

def _download_zip_to_tmp(bucket: str, key: str, tmp_path: str) -> int:
    """
    Descarga el ZIP de S3 a /tmp en chunks de EXTRACT_CHUNK_BYTES.
    Nunca carga el ZIP completo en RAM.
    Retorna el tamaño descargado en bytes.
    """
    logger.info(f"Downloading ZIP: s3://{bucket}/{key} → {tmp_path}")

    response  = s3.get_object(Bucket=bucket, Key=key)
    file_size = response.get('ContentLength', 0)
    bytes_dl  = 0

    with open(tmp_path, 'wb') as f:
        body = response['Body']
        while True:
            chunk = body.read(EXTRACT_CHUNK_BYTES)
            if not chunk:
                break
            f.write(chunk)
            bytes_dl += len(chunk)

    logger.info(f"Downloaded {bytes_dl / 1024 / 1024:.1f}MB "
                f"({file_size / 1024 / 1024:.1f}MB expected)")
    return bytes_dl


# =============================================================================
# INSPECCIÓN DEL ZIP SIN EXTRAER
# =============================================================================

def _inspect_zip(tmp_path: str) -> List[str]:
    """
    Lista los archivos dentro del ZIP sin extraerlos.
    Filtra carpetas y archivos ocultos.
    """
    with zipfile.ZipFile(tmp_path, 'r') as zf:
        all_names = zf.namelist()

    files = [
        name for name in all_names
        if not name.endswith('/')
        and not name.split('/')[-1].startswith('.')
        and name.split('/')[-1]
    ]

    logger.info(f"ZIP contains {len(all_names)} entries → {len(files)} processable files")
    for f in files:
        logger.info(f"  {f}")

    return files


# =============================================================================
# EXTRACCIÓN Y SUBIDA AL LANDING
# =============================================================================

def _extract_and_upload(
    tmp_zip_path: str,
    zip_entry_name: str,
    client_id: str,
    dest_bucket: str
) -> str:
    """
    Extrae un archivo del ZIP y lo sube al landing.
    Para archivos pequeños: upload simple.
    Para archivos grandes (>100MB): multipart upload.

    Destino en landing: {client_id}/{filename}
    Retorna el S3 key del archivo subido.
    """
    filename = zip_entry_name.split('/')[-1]
    dest_key = f"{client_id}/{filename}"

    with zipfile.ZipFile(tmp_zip_path, 'r') as zf:
        file_size = zf.getinfo(zip_entry_name).file_size

        logger.info(f"  Uploading: {filename} ({file_size / 1024 / 1024:.1f}MB) "
                    f"→ s3://{dest_bucket}/{dest_key}")

        if file_size < MULTIPART_THRESHOLD:
            # Upload simple para archivos < 100MB
            with zf.open(zip_entry_name) as entry:
                s3.put_object(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    Body=entry.read()
                )
        else:
            # Multipart upload para archivos >= 100MB
            mpu       = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
            upload_id = mpu['UploadId']
            parts     = []
            part_num  = 1
            buffer    = b''

            try:
                with zf.open(zip_entry_name) as entry:
                    while True:
                        chunk = entry.read(EXTRACT_CHUNK_BYTES)
                        if not chunk:
                            break
                        buffer += chunk

                        # Subir parte cuando alcanza 10MB (mínimo S3 = 5MB)
                        if len(buffer) >= 10 * 1024 * 1024:
                            response = s3.upload_part(
                                Bucket=dest_bucket,
                                Key=dest_key,
                                UploadId=upload_id,
                                PartNumber=part_num,
                                Body=buffer
                            )
                            parts.append({'PartNumber': part_num, 'ETag': response['ETag']})
                            logger.info(f"    Part {part_num}: {len(buffer) / 1024 / 1024:.1f}MB")
                            part_num += 1
                            buffer    = b''

                    # Subir el resto final
                    if buffer:
                        response = s3.upload_part(
                            Bucket=dest_bucket,
                            Key=dest_key,
                            UploadId=upload_id,
                            PartNumber=part_num,
                            Body=buffer
                        )
                        parts.append({'PartNumber': part_num, 'ETag': response['ETag']})

                s3.complete_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )

            except Exception as e:
                s3.abort_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id
                )
                raise

    logger.info(f"  Uploaded: {dest_key}")
    return dest_key


# =============================================================================
# ARCHIVO DEL ZIP ORIGINAL
# =============================================================================

def _archive_zip(
    source_bucket: str,
    source_key: str,
    client_id: str,
    file_date: str,
    dest_bucket: str
) -> str:
    """
    Archiva el ZIP original en operational/originals/zip/.
    Usa copy_object server-side — no descarga el archivo.

    Estructura:
      {client_id}/originals/zip/{year}/{month}/{zip_filename}
    """
    zip_filename = source_key.split('/')[-1]

    try:
        dt    = datetime.strptime(file_date, "%Y-%m-%d")
        year  = dt.strftime("%Y")
        month = dt.strftime("%m")
    except (ValueError, TypeError):
        now   = datetime.utcnow()
        year  = now.strftime("%Y")
        month = now.strftime("%m")

    archive_key = f"{client_id}/originals/zip/{year}/{month}/{zip_filename}"

    logger.info(f"Archiving ZIP → s3://{dest_bucket}/{archive_key}")
    s3.copy_object(
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Bucket=dest_bucket,
        Key=archive_key
    )
    logger.info("ZIP archived")
    return archive_key


def _delete_from_landing(bucket: str, key: str) -> None:
    logger.info(f"Deleting ZIP from landing: s3://{bucket}/{key}")
    s3.delete_object(Bucket=bucket, Key=key)
    logger.info("Landing clean")


def _cleanup_tmp(tmp_path: str) -> None:
    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except Exception as e:
        logger.warning(f"Could not clean tmp: {str(e)}")


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Entry point del Lambda unzip.
    Invocado asincrónicamente por itx-router cuando detecta un ZIP.

    Input (desde itx-router):
    {
        "client_id":      "EBGR",
        "bucket_landing": "itx-landing-dev",
        "s3_key":         "EBGR/VISA260416.zip",
        "file_date":      "2026-04-16"
    }

    Output:
    {
        "status":        "EXTRACTED",
        "zip_file":      "VISA260416.zip",
        "total_in_zip":  10,
        "matched":       4,
        "skipped":       6,
        "uploaded_keys": ["EBGR/I479273260330", ...],
        "archive_key":   "EBGR/originals/zip/2026/04/VISA260416.zip"
    }
    """
    logger.info("=" * 60)
    logger.info("ITX UNZIP LAMBDA - START")
    logger.info(f"Config: chunk={EXTRACT_CHUNK_BYTES // 1024 // 1024}MB")
    logger.info("=" * 60)

    if not LANDING_BUCKET:
        raise ValueError("Missing: S3_BUCKET_LANDING")
    if not OPERATIONAL_BUCKET:
        raise ValueError("Missing: S3_BUCKET_OPERATIONAL")

    client_id      = event.get('client_id')
    bucket_landing = event.get('bucket_landing', LANDING_BUCKET)
    s3_key         = event.get('s3_key')
    file_date      = event.get('file_date', datetime.utcnow().strftime("%Y-%m-%d"))

    if not all([client_id, s3_key]):
        raise ValueError(f"Missing required: client_id={client_id}, s3_key={s3_key}")

    zip_filename = s3_key.split('/')[-1]
    tmp_path     = f"/tmp/{zip_filename}"

    logger.info(f"Processing ZIP: {zip_filename}")
    logger.info(f"  Client:    {client_id}")
    logger.info(f"  Source:    s3://{bucket_landing}/{s3_key}")
    logger.info(f"  File date: {file_date}")

    try:
        # Paso 1 — Cargar patrones desde DynamoDB
        patterns = _load_patterns(client_id)
        if not patterns:
            raise ValueError(f"No active patterns for client '{client_id}'")

        # Paso 2 — Descargar ZIP a /tmp en streaming
        _download_zip_to_tmp(bucket_landing, s3_key, tmp_path)

        # Paso 3 — Inspeccionar contenido sin extraer
        all_files = _inspect_zip(tmp_path)

        # Paso 4 — Filtrar por patrones y extraer los que corresponden
        uploaded_keys = []
        skipped       = []

        for zip_entry in all_files:
            filename      = zip_entry.split('/')[-1]
            clasificacion = _matches_pattern(filename, patterns)

            if clasificacion:
                logger.info(f"  MATCH: {filename} "
                            f"({clasificacion['brand']}/{clasificacion['direction']})")
                dest_key = _extract_and_upload(
                    tmp_zip_path=tmp_path,
                    zip_entry_name=zip_entry,
                    client_id=client_id,
                    dest_bucket=bucket_landing
                )
                uploaded_keys.append(dest_key)
            else:
                logger.info(f"  SKIP:  {filename} (no pattern match)")
                skipped.append(filename)

        logger.info(f"Summary: {len(uploaded_keys)} uploaded, {len(skipped)} skipped")

        # Paso 5 — Archivar ZIP original en operational (server-side copy)
        archive_key = _archive_zip(
            source_bucket=bucket_landing,
            source_key=s3_key,
            client_id=client_id,
            file_date=file_date,
            dest_bucket=OPERATIONAL_BUCKET
        )

        # Paso 6 — Eliminar ZIP del landing
        _delete_from_landing(bucket_landing, s3_key)

        logger.info(f"=== Unzip complete — {len(uploaded_keys)} files sent to landing ===")
        logger.info(f"    S3 Events trigger router automatically for each file")

        return {
            'status':        'EXTRACTED',
            'zip_file':      zip_filename,
            'file_date':     file_date,
            'total_in_zip':  len(all_files),
            'matched':       len(uploaded_keys),
            'skipped':       len(skipped),
            'uploaded_keys': uploaded_keys,
            'archive_key':   archive_key,
        }

    finally:
        _cleanup_tmp(tmp_path)
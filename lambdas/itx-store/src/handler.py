"""
Lambda Store - itx-store
========================
Último paso del pipeline antes del archive.
Lee los 3 Parquets de staging (CLN + CAL + ITX) y los consolida
en un único Parquet final en el bucket operational.

Mapeo de subdirectorios (equivalente al store.py local):
  BASEII:
    300_baseii_cln_drafts  → transactions (clean)
    400_baseii_cal_drafts  → calculated   (Glue calculate)
    500_baseii_itx_drafts  → interchange  (Glue interchange)
    → operational: baseii_drafts

  SMS:
    300_sms_cln_messages   → transactions (clean)
    400_sms_cal_messages   → calculated   (Glue calculate)
    500_sms_itx_messages   → interchange  (Glue interchange)
    → operational: sms_messages

  VSS (110/120/130/140) — sin interchange:
    300_baseii_cln_vss_{type} → transactions (clean)
    400_baseii_cal_vss_{type} → calculated   (Glue calculate)
    → operational: baseii_vss_{type}

Variables de entorno:
  S3_BUCKET_STAGING     : bucket de staging (origen)
  S3_BUCKET_OPERATIONAL : bucket operational (destino)
  DYNAMODB_TABLE_FILE_CONTROL : tabla de control de archivos
"""

import os
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from typing import Optional, Dict, List

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

STAGING_BUCKET     = os.environ.get('S3_BUCKET_STAGING')
OPERATIONAL_BUCKET = os.environ.get('S3_BUCKET_OPERATIONAL')
FILE_CONTROL_TABLE = os.environ.get('DYNAMODB_TABLE_FILE_CONTROL', 'itx-file-control')

# =============================================================================
# MAPEO DE SUBDIRECTORIOS POR OUTPUT TYPE
# Equivalente a los parámetros de store_baseii_file, store_sms_file,
# store_vss_file en el código local.
# =============================================================================

OUTPUT_TYPE_CONFIG = {
    "BASEII": {
        "cln_subdir": "300_baseii_cln_drafts",
        "cal_subdir": "400_baseii_cal_drafts",
        "itx_subdir": "500_baseii_itx_drafts",
        "target_subdir": "baseii_drafts",
        "has_interchange": True,
    },
    "SMS": {
        "cln_subdir": "300_sms_cln_messages",
        "cal_subdir": "400_sms_cal_messages",
        "itx_subdir": "500_sms_itx_messages",
        "target_subdir": "sms_messages",
        "has_interchange": True,
    },
    "VSS_110": {
        "cln_subdir": "300_vss_110_cln",
        "cal_subdir": "400_vss_110_cal",
        "itx_subdir": None,
        "target_subdir": "vss_110",
        "has_interchange": False,
    },
    "VSS_120": {
        "cln_subdir": "300_vss_120_cln",
        "cal_subdir": "400_vss_120_cal",
        "itx_subdir": None,
        "target_subdir": "vss_120",
        "has_interchange": False,
    },
    "VSS_130": {
        "cln_subdir": "300_vss_130_cln",
        "cal_subdir": "400_vss_130_cal",
        "itx_subdir": None,
        "target_subdir": "vss_130",
        "has_interchange": False,
    },
    "VSS_140": {
        "cln_subdir": "300_vss_140_cln",
        "cal_subdir": "400_vss_140_cal",
        "itx_subdir": None,
        "target_subdir": "vss_140",
        "has_interchange": False,
    },
}

# =============================================================================
# HELPERS
# =============================================================================

def _read_parquet_from_s3(bucket: str, s3_key: str) -> pd.DataFrame:
    """
    Lee un Parquet de S3.
    Soporta archivo único (Lambda output) y directorio PySpark (Glue output).
    """
    logger.info(f"Reading s3://{bucket}/{s3_key}")

    # Intentar como archivo único primero (CLN — output de Lambda)
    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        table = pq.read_table(BytesIO(response['Body'].read()))
        logger.info(f"  Read as single file: {len(table):,} rows")
        return table.to_pandas()
    except Exception as e:
        if 'NoSuchKey' not in str(e) and '404' not in str(e):
            raise  # error distinto a "no existe" — relanzar

    # Fallback: es un directorio PySpark con part files
    prefix = s3_key if s3_key.endswith('/') else s3_key + '/'
    logger.info(f"  Not a single file — scanning part files in {prefix}")

    paginator = s3.get_paginator('list_objects_v2')
    part_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.parquet') and 'part-' in key:
                part_keys.append(key)

    if not part_keys:
        raise FileNotFoundError(
            f"No part files found in s3://{bucket}/{prefix}"
        )

    logger.info(f"  Found {len(part_keys)} part files — reading...")
    tables = []
    for key in sorted(part_keys):
        response = s3.get_object(Bucket=bucket, Key=key)
        tables.append(pq.read_table(BytesIO(response['Body'].read())))
        logger.info(f"    {key.split('/')[-1]}: {len(tables[-1]):,} rows")

    merged = pa.concat_tables(tables)
    logger.info(f"  Total after concat: {len(merged):,} rows")
    return merged.to_pandas()


def _write_parquet_to_s3(df: pd.DataFrame, bucket: str, s3_key: str) -> int:
    """Escribe un DataFrame como Parquet en S3. Retorna el número de records."""
    logger.info(f"Writing {len(df):,} records to s3://{bucket}/{s3_key}")
    try:
        table  = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())
        logger.info(f"Saved {len(df):,} records → s3://{bucket}/{s3_key}")
        return len(df)
    except Exception as e:
        logger.error(f"Error writing s3://{bucket}/{s3_key}: {str(e)}")
        raise

def _read_parquet_arrow(bucket: str, key: str) -> pa.Table:
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return pq.read_table(BytesIO(response['Body'].read()))
    except s3.exceptions.NoSuchKey:
        pass

    prefix = key if key.endswith('/') else key + '/'

    paginator = s3.get_paginator('list_objects_v2')
    tables = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            k = obj['Key']
            if k.endswith('.parquet') and 'part-' in k:
                r = s3.get_object(Bucket=bucket, Key=k)
                tables.append(pq.read_table(BytesIO(r['Body'].read())))

    return pa.concat_tables(tables, mode="default")



def _build_s3_key(
    clean_s3_key: str,
    cln_subdir: str,
    target_subdir: str,
    bucket_type: str = "operational"
) -> str:
    """
    Construye el S3 key de destino reemplazando el subdir.

    Para staging (cal e itx): reemplaza 300_xxx → 400_xxx o 500_xxx
    Para operational (target): reemplaza 300_xxx → baseii_drafts/etc
    y cambia el path prefix para apuntar a operational.

    Ejemplo:
      clean_s3_key = "SBSA/VISA/300_baseii_cln_drafts/file_type=OUT/date=2026-04-04/HASH.parquet"
      cln_subdir   = "300_baseii_cln_drafts"
      target_subdir = "baseii_drafts"
      → "SBSA/VISA/baseii_drafts/file_type=OUT/date=2026-04-04/HASH.parquet"
    """
    return clean_s3_key.replace(cln_subdir, target_subdir)

# =============================================================================
# FUNCIÓN PRINCIPAL DE STORE POR OUTPUT TYPE
# =============================================================================

def store_output(
    output: Dict,
    client_id: str,
    brand: str,
    file_type: str,
    file_date: str,
    content_hash: str,
) -> Optional[Dict]:
    """
    Realiza el merge de los 3 Parquets (CLN + CAL + ITX) para un output_type
    y guarda el resultado en operational.

    Estrategia de memoria:
      - CAL e ITX se cargan completos en RAM (pocas columnas ~50)
      - CLN se procesa en chunks (muchas columnas ~250)
      - Join por índice 'record' — resultado idéntico al join completo
    """
    import time

    output_type  = output.get('output_type')
    clean_s3_key = output.get('s3_key')

    logger.info(f"{'='*60}")
    logger.info(f"Processing store for: {output_type}")

    config = OUTPUT_TYPE_CONFIG.get(output_type)
    if not config:
        logger.warning(f"No config found for output_type: {output_type} — skipping")
        return None

    cln_subdir      = config['cln_subdir']
    cal_subdir      = config['cal_subdir']
    itx_subdir      = config['itx_subdir']
    target_subdir   = config['target_subdir']
    has_interchange = config['has_interchange']

    try:
        # ── Construir S3 keys ─────────────────────────────────────────────
        cal_s3_key    = clean_s3_key.replace(cln_subdir, cal_subdir)
        target_s3_key = clean_s3_key.replace(cln_subdir, target_subdir)

        # ── Paso 1: CAL completo en RAM (pocas columnas ~50) ──────────────
        t0 = time.time()
        logger.info("Loading CAL into memory...")
        cal_df = _read_parquet_from_s3(STAGING_BUCKET, cal_s3_key)
        if 'record' in cal_df.columns:
            cal_df = cal_df.set_index('record')
        logger.info(f"  CAL: {len(cal_df):,} rows, {len(cal_df.columns)} cols [{time.time()-t0:.1f}s]")

        # ── Paso 2: ITX completo en RAM (pocas columnas, solo BASEII/SMS) ─
        itx_df = None
        if has_interchange and itx_subdir:
            itx_s3_key = clean_s3_key.replace(cln_subdir, itx_subdir)
            t1 = time.time()
            logger.info("Loading ITX into memory...")
            itx_df = _read_parquet_from_s3(STAGING_BUCKET, itx_s3_key)
            if 'record' in itx_df.columns:
                itx_df = itx_df.set_index('record')
            logger.info(f"  ITX: {len(itx_df):,} rows, {len(itx_df.columns)} cols [{time.time()-t1:.1f}s]")
        else:
            logger.info(f"ITX skipped for {output_type} (VSS — no interchange)")

        # ── Paso 3: CLN abierto para chunking (NO se carga completo) ──────
        t2 = time.time()
        logger.info("Opening CLN for chunked processing...")
        response     = s3.get_object(Bucket=STAGING_BUCKET, Key=clean_s3_key)
        file_obj     = BytesIO(response['Body'].read())
        parquet_file = pq.ParquetFile(file_obj)
        logger.info(f"  CLN opened [{time.time()-t2:.1f}s]")

        chunk_size      = int(os.environ.get('STORE_CHUNK_SIZE', '80000'))
        output_buffer   = BytesIO()
        writer          = None
        schema          = None   # se captura en el primer batch
        records_written = 0
        output_cols     = 0
        batch_num       = 0

        # ── Paso 4: Procesar CLN en chunks, join con CAL e ITX ────────────
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            chunk_df = batch.to_pandas()
            if chunk_df.empty:
                continue

            batch_num += 1
            t_batch = time.time()

            if 'record' in chunk_df.columns:
                chunk_df = chunk_df.set_index('record')

            # Join CLN chunk + CAL (por índice 'record')
            cal_chunk = cal_df.loc[cal_df.index.intersection(chunk_df.index)]
            merged    = chunk_df.join(cal_chunk, how='left', lsuffix='_cln')

            # Join con ITX (solo BASEII y SMS)
            if itx_df is not None:
                itx_chunk = itx_df.loc[itx_df.index.intersection(chunk_df.index)]
                merged    = merged.join(itx_chunk, how='left', rsuffix='_itx')

            merged = merged.reset_index()

            for col_name in merged.select_dtypes(include='object').columns:
                merged[col_name] = merged[col_name].where(pd.notna(merged[col_name]), other=None)

            # Convertir a PyArrow
            merged_table = pa.Table.from_pandas(merged)

            if writer is None:
                # Primer batch — capturar schema como referencia
                schema      = merged_table.schema
                writer      = pq.ParquetWriter(
                    output_buffer, schema, compression='snappy'
                )
                output_cols = len(merged.columns)
            else:
                # Batches siguientes — forzar schema del primero
                # Evita el error "Table schema does not match" entre chunks
                try:
                    merged_table = merged_table.cast(schema)
                except Exception:
                    merged_table = pa.Table.from_pandas(
                        merged, schema=schema, safe=False
                    )

            writer.write_table(merged_table)
            records_written += len(merged)
            logger.info(f"  Batch {batch_num}: +{len(merged):,} records "
                        f"(total: {records_written:,}) [{time.time()-t_batch:.1f}s]")

        if writer is None:
            logger.warning(f"No records written for {output_type}")
            file_obj.close()
            return None

        writer.close()
        output_buffer.seek(0)

        # ── Paso 5: Subir Parquet final a operational ──────────────────────
        logger.info(f"Uploading to s3://{OPERATIONAL_BUCKET}/{target_s3_key}")
        s3.put_object(
            Bucket=OPERATIONAL_BUCKET,
            Key=target_s3_key,
            Body=output_buffer.getvalue()
        )
        logger.info(f"Saved {records_written:,} records, {output_cols} cols → {target_s3_key}")

        file_obj.close()
        output_buffer.close()

        return {
            'output_type':   output_type,
            'cln_s3_key':    clean_s3_key,
            'cal_s3_key':    cal_s3_key,
            'itx_s3_key':    clean_s3_key.replace(cln_subdir, itx_subdir) if has_interchange and itx_subdir else None,
            'target_s3_key': target_s3_key,
            'records':       records_written,
            'columns':       output_cols,
            'batches':       batch_num,
        }

    except Exception as e:
        logger.error(f"Error storing {output_type}: {str(e)}", exc_info=True)
        raise

# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Entry point del Lambda store.

    Input (desde Step Functions — viene del clean_result.outputs):
    {
        "client_id":    "SBSA",
        "file_id":      "ABC123",
        "brand":        "VISA",
        "file_type":    "OUT",
        "file_date":    "2026-04-04",
        "content_hash": "279AE7CB...",
        "outputs": [
            {
                "output_type":   "BASEII",
                "output_subdir": "300_baseii_cln_drafts",
                "s3_key":        "SBSA/VISA/300_baseii_cln_drafts/file_type=OUT/date=.../HASH.parquet",
                "records":       2369332,
                ...
            },
            ...
        ]
    }

    Output:
    {
        "status":        "SUCCESS",
        "total_outputs": 1,
        "total_records": 2369332,
        "outputs": [...],
        "client_id":     "SBSA",
        ...
    }
    """
    logger.info("=" * 70)
    logger.info("ITX STORE LAMBDA - START")
    logger.info("=" * 70)

    if not STAGING_BUCKET:
        raise ValueError("Missing environment variable: S3_BUCKET_STAGING")
    if not OPERATIONAL_BUCKET:
        raise ValueError("Missing environment variable: S3_BUCKET_OPERATIONAL")

    client_id    = event.get('client_id')
    file_id      = event.get('file_id')
    brand        = event.get('brand')
    file_type    = event.get('file_type')
    file_date    = event.get('file_date')
    content_hash = event.get('content_hash')

    clean_outputs = event.get('outputs', [])

    if not clean_outputs:
        logger.warning("No outputs received — nothing to store")
        return {'status': 'SUCCESS', 'total_outputs': 0, 'outputs': []}

    logger.info(f"Processing store for: client={client_id}, brand={brand}, "
                f"type={file_type}, date={file_date}")
    logger.info(f"Outputs to store: {[o.get('output_type') for o in clean_outputs]}")

    store_outputs = []
    errors        = []

    for output in clean_outputs:
        try:
            result = store_output(
                output=output,
                client_id=client_id,
                brand=brand,
                file_type=file_type,
                file_date=file_date,
                content_hash=content_hash,
            )
            if result:
                store_outputs.append(result)
        except Exception as e:
            errors.append({
                'output_type': output.get('output_type'),
                'error': str(e),
            })

    total_records = sum(o.get('records', 0) for o in store_outputs)

    status = ('ERROR'           if (errors and not store_outputs) else
              'PARTIAL_SUCCESS' if errors else
              'SUCCESS')

    logger.info(f"=== Done: {len(store_outputs)} outputs, "
                f"{total_records:,} records stored to operational ===")

    return {
        'status':        status,
        'total_outputs': len(store_outputs),
        'total_records': total_records,
        'outputs':       store_outputs,
        'errors':        errors if errors else None,
        'client_id':     client_id,
        'file_id':       file_id,
        'brand':         brand,
        'file_type':     file_type,
        'file_date':     file_date,
        'content_hash':  content_hash,
    }
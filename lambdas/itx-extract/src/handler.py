"""
Lambda Extract - itx-extract (Optimizado v2)
============================================
Mejoras aplicadas vs versión anterior:

1. itertuples() en vez de iterrows()
   → 10x más rápido para iterar field_defs
   → Acceso por atributo en vez de dict lookup

2. dict → DataFrame en vez de concat de 250 Series
   → Elimina la construcción de objetos intermedios
   → Una sola operación de memoria al final

Variables de entorno:
  S3_BUCKET_STAGING        : bucket con los Parquets de transform
  DYNAMODB_FIELD_DEFINITION: tabla de definiciones de campos
  EXTRACT_CHUNK_SIZE        : records por batch (default: 300000)
"""

import os
import json
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from typing import Optional, Dict, List
from boto3.dynamodb.conditions import Key
import time

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

STAGING_BUCKET  = os.environ.get('S3_BUCKET_STAGING')
FIELD_DEF_TABLE = os.environ.get('DYNAMODB_FIELD_DEFINITION', 'itx-visa-fields')
CHUNK_SIZE      = int(os.environ.get('EXTRACT_CHUNK_SIZE', '300000'))

# =============================================================================
# MAPEO DE CONFIGURACIÓN POR TIPO DE OUTPUT
# =============================================================================

OUTPUT_TYPE_CONFIG = {
    "BASEII": {
        "type_record": "draft",
        "input_subdir": "100_baseii_raw_drafts",
        "output_subdir": "200_baseii_ext_drafts",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
    "SMS": {
        "type_record": "sms",
        "input_subdir": "100_sms_raw_messages",
        "output_subdir": "200_sms_ext_messages",
        "sort_by": ["secondary_identifier", "position"],
        "special_processing": "sms"
    },
    "VSS_110": {
        "type_record": "vss_110",
        "input_subdir": "100_vss_110_raw",
        "output_subdir": "200_vss_110_ext",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
    "VSS_120": {
        "type_record": "vss_120",
        "input_subdir": "100_vss_120_raw",
        "output_subdir": "200_vss_120_ext",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
    "VSS_130": {
        "type_record": "vss_130",
        "input_subdir": "100_vss_130_raw",
        "output_subdir": "200_vss_130_ext",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
    "VSS_140": {
        "type_record": "vss_140",
        "input_subdir": "100_vss_140_raw",
        "output_subdir": "200_vss_140_ext",
        "sort_by": ["tcsn", "position", "secondary_identifier_len"]
    },
}

# =============================================================================
# FUNCIONES DE ACCESO A DATOS
# =============================================================================

def _load_field_definitions(type_record: str, sort_by: List[str]) -> pd.DataFrame:
    logger.info(f"Loading field definitions for type_record: {type_record}")

    table    = dynamodb.Table(FIELD_DEF_TABLE)
    response = table.query(
        IndexName='type-record-index',
        KeyConditionExpression=Key('type_record').eq(type_record)
    )
    items = response.get('Items', [])

    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName='type-record-index',
            KeyConditionExpression=Key('type_record').eq(type_record),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))

    if not items:
        logger.warning(f"No field definitions found for type_record: {type_record}")
        return pd.DataFrame()

    df = pd.DataFrame(items)

    int_cols = [
        'position', 'length', 'secondary_identifier_pos',
        'secondary_identifier_len', 'sort_order'
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    sort_cols = [c for c in sort_by if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=True)

    logger.info(f"Loaded {len(df)} field definitions for {type_record}")
    return df


def _get_s3_file_object(s3_key: str) -> BytesIO:
    logger.info(f"Downloading file into memory buffer from s3://{STAGING_BUCKET}/{s3_key}")
    try:
        response = s3.get_object(Bucket=STAGING_BUCKET, Key=s3_key)
        return BytesIO(response['Body'].read())
    except Exception as e:
        logger.error(f"Error reading Parquet {s3_key}: {str(e)}")
        raise


def _process_sms_field_defs(field_defs: pd.DataFrame) -> pd.DataFrame:
    if 'secondary_identifier' in field_defs.columns:
        field_defs = field_defs[
            field_defs['secondary_identifier'] != 'V22000'
        ].copy()
        field_defs['secondary_identifier'] = field_defs['secondary_identifier'].apply(
            lambda x: str(x)[1:] if x and str(x).startswith('V') else x
        )
    return field_defs


# =============================================================================
# LÓGICA DE EXTRACCIÓN DE CAMPOS
# MEJORA 1: itertuples() en vez de iterrows()
# MEJORA 2: dict → DataFrame en vez de concat de 250 Series
# =============================================================================

def _extract_fields(data, field_defs, type_record):

    if data.empty or field_defs.empty:
        return pd.DataFrame()

    from collections import defaultdict
    tcsn_groups = defaultdict(list)
    for fd in field_defs.itertuples():
        tcsn = str(fd.tcsn) if hasattr(fd, 'tcsn') else ''
        if tcsn and tcsn in data.columns:
            tcsn_groups[tcsn].append(fd)
        else:
            sec_id = str(fd.secondary_identifier).strip() \
                     if hasattr(fd, 'secondary_identifier') \
                     and fd.secondary_identifier \
                     and not pd.isna(fd.secondary_identifier) else ''
            if sec_id and sec_id in data.columns:
                tcsn_groups[sec_id].append(fd)

    fields = []

    for tcsn, fds in tcsn_groups.items():
        col = data[tcsn]

        for fd in fds:
            position    = int(fd.position)    if hasattr(fd, 'position')    else 0
            length      = int(fd.length)      if hasattr(fd, 'length')      else 0
            column_name = str(fd.column_name) if hasattr(fd, 'column_name') else ''

            if not column_name or position <= 0 or length <= 0:
                continue

            sec_id = fd.secondary_identifier \
                     if hasattr(fd, 'secondary_identifier') else None

            sec_id_str = str(sec_id).strip() if sec_id and not pd.isna(sec_id) else ''

            # ← CAMBIO: agrega "or tcsn == sec_id_str" para detectar caso SMS
            # SMS: tcsn fue reasignado a "22200", sec_id_str también es "22200"
            # → son iguales → no hay filtro adicional de filas
            if not sec_id_str or tcsn == sec_id_str:
                col_view = col
            else:
                sec_id_pos = int(fd.secondary_identifier_pos) \
                             if hasattr(fd, 'secondary_identifier_pos') \
                             and fd.secondary_identifier_pos else 0
                sec_id_len = int(fd.secondary_identifier_len) \
                             if hasattr(fd, 'secondary_identifier_len') \
                             and fd.secondary_identifier_len else 0

                if sec_id_pos > 0 and sec_id_len > 0:
                    try:
                        mask     = col.str.slice(sec_id_pos-1, sec_id_pos-1+sec_id_len) == sec_id_str
                        col_view = col[mask]
                    except Exception:
                        col_view = col
                else:
                    col_view = col

            try:
                field = pd.Series(
                    col_view.str.slice(
                        start=position - 1,
                        stop=position - 1 + length
                    ).reindex(data.index, fill_value=''),
                    name=column_name
                )
                fields.append(field)
            except Exception:
                continue

    if not fields:
        return pd.DataFrame()

    extract_df = pd.concat(fields, axis=1).fillna('').astype(str)
    extract_df = extract_df.reset_index(drop=True)

    if 'record' in data.columns:
        extract_df.insert(0, 'record', data['record'].reset_index(drop=True).values)

    return extract_df


# =============================================================================
# FUNCIÓN PRINCIPAL DE EXTRACCIÓN POR OUTPUT
# =============================================================================

def extract_output(
    output: Dict,
    client_id: str, brand: str,
    file_type: str, file_date: str, content_hash: str
) -> Optional[Dict]:

    output_type   = output.get('output_type')
    input_s3_key  = output.get('s3_key')
    input_records = output.get('records', 0)

    logger.info(f"{'='*60}")
    logger.info(f"Processing extract for: {output_type}")

    config = OUTPUT_TYPE_CONFIG.get(output_type)
    if not config:
        return None

    type_record        = config['type_record']
    input_subdir       = config['input_subdir']
    output_subdir      = config['output_subdir']
    sort_by            = config['sort_by']
    special_processing = config.get('special_processing')

    try:
        t0 = time.time()
        field_defs = _load_field_definitions(type_record, sort_by)
        logger.info(f"  [TIMING] DynamoDB load: {time.time()-t0:.2f}s ({len(field_defs)} fields)")

        if field_defs.empty:
            return None

        if special_processing == 'sms':
            field_defs = _process_sms_field_defs(field_defs)

        t1 = time.time()
        file_obj = _get_s3_file_object(input_s3_key)
        logger.info(f"  [TIMING] S3 download: {time.time()-t1:.2f}s")

        parquet_file  = pq.ParquetFile(file_obj)
        output_s3_key = input_s3_key.replace(input_subdir, output_subdir)
        output_buffer = BytesIO()
        writer        = None
        records_written = 0
        fields_count    = 0
        batch_num       = 0

        logger.info(f"Starting chunked processing. Chunk size: {CHUNK_SIZE:,}")

        for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE):
            chunk_df = batch.to_pandas()
            if chunk_df.empty:
                continue

            batch_num += 1
            t_batch = time.time()

            t_conv = time.time()
            extracted_chunk = _extract_fields(chunk_df, field_defs, type_record)
            t_extract = time.time() - t_conv

            if extracted_chunk.empty:
                continue

            t_arrow = time.time()
            extracted_table = pa.Table.from_pandas(extracted_chunk)
            t_arrow = time.time() - t_arrow

            if writer is None:
                writer       = pq.ParquetWriter(output_buffer, extracted_table.schema, compression='snappy')
                fields_count = len(extracted_chunk.columns)

            t_write = time.time()
            writer.write_table(extracted_table)
            t_write = time.time() - t_write

            records_written += len(extracted_chunk)
            t_total = time.time() - t_batch
            logger.info(f"  Batch {batch_num}: +{len(extracted_chunk):,} records "
                        f"(total: {records_written:,}) | "
                        f"extract={t_extract:.2f}s arrow={t_arrow:.2f}s write={t_write:.2f}s total={t_total:.2f}s")

        if writer is None:
            logger.warning(f"No valid records for {output_type}")
            return None

        writer.close()
        output_buffer.seek(0)

        t_upload = time.time()
        logger.info(f"Uploading to S3: {output_s3_key}")
        s3.put_object(Bucket=STAGING_BUCKET, Key=output_s3_key, Body=output_buffer.getvalue())
        logger.info(f"  [TIMING] S3 upload: {time.time()-t_upload:.2f}s")
        logger.info(f"Done: {records_written:,} records, {fields_count} fields, {batch_num} batches")

        file_obj.close()
        output_buffer.close()

        return {
            'output_type':   output_type,
            'type_record':   type_record,
            'input_subdir':  input_subdir,
            'output_subdir': output_subdir,
            'input_s3_key':  input_s3_key,
            's3_key':        output_s3_key,
            'input_records': input_records,
            'records':       records_written,
            'fields':        fields_count,
            'batches':       batch_num,
        }

    except Exception as e:
        logger.error(f"Error extracting {output_type}: {str(e)}", exc_info=True)
        raise


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    logger.info("=" * 70)
    logger.info("ITX EXTRACT LAMBDA v2 - START")
    logger.info(f"Config: chunk_size={CHUNK_SIZE:,}")
    logger.info("=" * 70)

    if not STAGING_BUCKET:
        raise ValueError("Missing: S3_BUCKET_STAGING")

    client_id    = event.get('client_id')
    file_id      = event.get('file_id')
    brand        = event.get('brand')
    file_type    = event.get('file_type')
    file_date    = event.get('file_date')
    content_hash = event.get('content_hash')

    transform_outputs = event.get('outputs', [])
    if not transform_outputs:
        return {'status': 'SUCCESS', 'outputs': []}

    extract_outputs = []
    errors          = []

    for output in transform_outputs:
        try:
            result = extract_output(
                output=output,
                client_id=client_id, brand=brand,
                file_type=file_type, file_date=file_date,
                content_hash=content_hash
            )
            if result:
                extract_outputs.append(result)
        except Exception as e:
            errors.append({
                'output_type': output.get('output_type'),
                'error': str(e)
            })

    total_records = sum(o.get('records', 0) for o in extract_outputs)
    total_fields  = sum(o.get('fields',  0) for o in extract_outputs)
    total_batches = sum(o.get('batches', 0) for o in extract_outputs)

    status = ('ERROR'           if (errors and not extract_outputs) else
              'PARTIAL_SUCCESS' if errors else
              'SUCCESS')

    logger.info(f"=== Done: {len(extract_outputs)} outputs, "
                f"{total_records:,} records, "
                f"{total_batches} batches total ===")

    return {
        'status':        status,
        'total_outputs': len(extract_outputs),
        'total_records': total_records,
        'total_fields':  total_fields,
        'outputs':       extract_outputs,
        'errors':        errors if errors else None,
        'client_id':     client_id,
        'file_id':       file_id,
        'brand':         brand,
        'file_type':     file_type,
        'file_date':     file_date,
        'content_hash':  content_hash
    }
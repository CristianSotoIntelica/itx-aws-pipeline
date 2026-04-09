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

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clientes AWS
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Variables de entorno
STAGING_BUCKET = os.environ.get('S3_BUCKET_STAGING')
FIELD_DEF_TABLE = os.environ.get('DYNAMODB_FIELD_DEFINITION', 'itx-visa-fields')

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
# FUNCIONES DE ACCESO A DATOS (Adaptado para PyArrow)
# =============================================================================

def _load_field_definitions(type_record: str, sort_by: List[str]) -> pd.DataFrame:
    logger.info(f"Loading field definitions for type_record: {type_record}")
    
    table = dynamodb.Table(FIELD_DEF_TABLE)
    
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
    
    return df

def _get_s3_file_object(s3_key: str) -> BytesIO:
    """Descarga el Parquet en un buffer de memoria para procesarlo por lotes."""
    logger.info(f"Downloading file into memory buffer from s3://{STAGING_BUCKET}/{s3_key}")
    try:
        response = s3.get_object(Bucket=STAGING_BUCKET, Key=s3_key)
        return BytesIO(response['Body'].read())
    except Exception as e:
        logger.error(f"Error reading Parquet {s3_key}: {str(e)}")
        raise

# =============================================================================
# LÓGICA DE EXTRACCIÓN DE CAMPOS (Intacta)
# =============================================================================

def _extract_fields(data: pd.DataFrame, field_defs: pd.DataFrame, type_record: str) -> pd.DataFrame:
    fields = []
    
    for idx, fd in field_defs.iterrows():
        tcsn = str(fd.get('tcsn', ''))
        position = int(fd.get('position', 0))
        length = int(fd.get('length', 0))
        column_name = fd.get('column_name', '')
        
        if not tcsn or not column_name or position <= 0 or length <= 0:
            continue
        
        if tcsn not in data.columns:
            continue
        
        sec_id = fd.get('secondary_identifier')
        sec_id_pos = fd.get('secondary_identifier_pos')
        sec_id_len = fd.get('secondary_identifier_len')
        
        if not sec_id or pd.isna(sec_id) or str(sec_id).strip() == '':
            data_view = data
        else:
            sec_id_pos = int(sec_id_pos) if sec_id_pos and not pd.isna(sec_id_pos) else 0
            sec_id_len = int(sec_id_len) if sec_id_len and not pd.isna(sec_id_len) else 0
            
            if sec_id_pos > 0 and sec_id_len > 0:
                try:
                    data_view = data[
                        data[tcsn].str.slice(
                            start=sec_id_pos - 1,
                            stop=sec_id_pos - 1 + sec_id_len
                        ) == str(sec_id)
                    ]
                except Exception:
                    data_view = data
            else:
                data_view = data
        
        try:
            field = pd.Series(
                data_view[tcsn].str.slice(
                    start=position - 1,
                    stop=position - 1 + length
                ),
                name=column_name
            )
            fields.append(field)
        except Exception:
            continue
    
    if not fields:
        return pd.DataFrame()
    
    extract_df = pd.concat(fields, axis=1).fillna('').astype(str)

    # 🌟 RESCATE DE LA COLUMNA RECORD 🌟
    if 'record' in data.columns:
        extract_df.insert(0, 'record', data['record'])
    
    return extract_df

def _process_sms_field_defs(field_defs: pd.DataFrame) -> pd.DataFrame:
    if 'secondary_identifier' in field_defs.columns:
        field_defs = field_defs[field_defs['secondary_identifier'] != 'V22000'].copy()
        field_defs['secondary_identifier'] = field_defs['secondary_identifier'].apply(
            lambda x: str(x)[1:] if x and str(x).startswith('V') else x
        )
    return field_defs

# =============================================================================
# FUNCIÓN PRINCIPAL DE EXTRACCIÓN POR OUTPUT (Ahora con Chunking PyArrow)
# =============================================================================

def extract_output(output: Dict, client_id: str, brand: str, 
                   file_type: str, file_date: str, content_hash: str) -> Optional[Dict]:
    output_type = output.get('output_type')
    input_s3_key = output.get('s3_key')
    input_records = output.get('records', 0)
    
    logger.info(f"{'='*60}")
    logger.info(f"Processing extract for: {output_type}")
    
    config = OUTPUT_TYPE_CONFIG.get(output_type)
    if not config:
        return None
    
    type_record = config['type_record']
    input_subdir = config['input_subdir']
    output_subdir = config['output_subdir']
    sort_by = config['sort_by']
    special_processing = config.get('special_processing')
    
    try:
        field_defs = _load_field_definitions(type_record, sort_by)
        if field_defs.empty:
            return None
        
        if special_processing == 'sms':
            field_defs = _process_sms_field_defs(field_defs)
        
        # 1. Descargar y preparar ParquetFile de PyArrow
        file_obj = _get_s3_file_object(input_s3_key)
        parquet_file = pq.ParquetFile(file_obj)
        
        output_s3_key = input_s3_key.replace(input_subdir, output_subdir)
        output_buffer = BytesIO()
        writer = None
        
        records_written = 0
        chunk_size = int(os.environ.get('EXTRACT_CHUNK_SIZE', '400000'))
        fields_count = 0
        
        logger.info(f"Starting chunked processing. Chunk size: {chunk_size}")

        # 2. Iterar en Lotes (Chunking)
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            chunk_df = batch.to_pandas()
            
            if chunk_df.empty:
                continue
                
            extracted_chunk = _extract_fields(chunk_df, field_defs, type_record)
            
            if extracted_chunk.empty:
                continue
            
            # Convertir el DataFrame procesado de vuelta a tabla de PyArrow
            extracted_table = pa.Table.from_pandas(extracted_chunk)
            
            # Inicializar el Writer con el esquema del primer lote
            if writer is None:
                writer = pq.ParquetWriter(output_buffer, extracted_table.schema, compression='snappy')
                fields_count = len(extracted_chunk.columns)
                
            writer.write_table(extracted_table)
            records_written += len(extracted_chunk)
            logger.info(f"  Processed batch. Total records so far: {records_written}")
            
        # 3. Finalizar y subir a S3
        if writer is not None:
            writer.close()
            output_buffer.seek(0)
            
            logger.info(f"Uploading full processed Parquet to S3: {output_s3_key}")
            s3.put_object(
                Bucket=STAGING_BUCKET,
                Key=output_s3_key,
                Body=output_buffer.getvalue()
            )
        else:
            logger.warning(f"No valid records processed for {output_type}")
            return None

        # Liberar memoria
        file_obj.close()
        output_buffer.close()

        result = {
            'output_type': output_type,
            'type_record': type_record,
            'input_subdir': input_subdir,
            'output_subdir': output_subdir,
            'input_s3_key': input_s3_key,
            's3_key': output_s3_key,
            'input_records': input_records,
            'records': records_written,
            'fields': fields_count
        }
        return result
        
    except Exception as e:
        logger.error(f"Error extracting {output_type}: {str(e)}", exc_info=True)
        raise

# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    logger.info("=" * 70)
    logger.info("ITX EXTRACT LAMBDA - START (WITH CHUNKING)")
    logger.info("=" * 70)
    
    if not STAGING_BUCKET:
        raise ValueError("Missing required environment variable: S3_BUCKET_STAGING")
    
    client_id = event.get('client_id')
    file_id = event.get('file_id')
    brand = event.get('brand')
    file_type = event.get('file_type')
    file_date = event.get('file_date')
    content_hash = event.get('content_hash')
    
    transform_outputs = event.get('outputs', [])
    
    if not transform_outputs:
        return {'status': 'SUCCESS', 'outputs': []}
    
    extract_outputs = []
    errors = []
    
    for i, output in enumerate(transform_outputs):
        try:
            result = extract_output(
                output=output, client_id=client_id, brand=brand,
                file_type=file_type, file_date=file_date, content_hash=content_hash
            )
            if result:
                extract_outputs.append(result)
        except Exception as e:
            errors.append({'output_type': output.get('output_type'), 'error': str(e)})
    
    total_records = sum(o.get('records', 0) for o in extract_outputs)
    total_fields = sum(o.get('fields', 0) for o in extract_outputs)
    
    status = 'ERROR' if (errors and not extract_outputs) else ('PARTIAL_SUCCESS' if errors else 'SUCCESS')
    
    return {
        'status': status,
        'total_outputs': len(extract_outputs),
        'total_records': total_records,
        'total_fields': total_fields,
        'outputs': extract_outputs,
        'errors': errors if errors else None,
        'client_id': client_id,
        'file_id': file_id,
        'brand': brand,
        'file_type': file_type,
        'file_date': file_date,
        'content_hash': content_hash
    }

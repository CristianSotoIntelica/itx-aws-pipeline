"""
Lambda Router - itx-router
===========================
Trigger: S3 Event Notification cuando llega un archivo a Landing.

Flujo:
1. Parsear evento S3 → bucket/key
2. Extraer client_id del path
3. Detectar si es ZIP → delegar a itx-unzip asincrónicamente
4. Cargar patrones de DynamoDB
5. Clasificar archivo con regex
6. Extraer fecha del header (solo 50 bytes, sin descargar todo)
7. Calcular MD5 en streaming (sin cargar todo el archivo en memoria)
8. Verificar duplicado en DynamoDB
9. Registrar en DynamoDB
10. Iniciar Step Functions

Variables de entorno:
  S3_BUCKET_LANDING          : bucket de landing
  DYNAMODB_TABLE_FILE_CONTROL: tabla de control (default: itx-file-control)
  DYNAMODB_TABLE_FILE_PATTERN: tabla de patrones (default: itx-file-pattern)
  STEP_FUNCTION_ARN          : ARN de la Step Function principal
  UNZIP_FUNCTION_NAME        : nombre de la Lambda unzip (default: itx-unzip)
"""

import os
import re
import json
import hashlib
import logging
import boto3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3            = boto3.client('s3')
dynamodb      = boto3.resource('dynamodb')
sfn           = boto3.client('stepfunctions')
lambda_client = boto3.client('lambda')

LANDING_BUCKET      = os.environ.get('S3_BUCKET_LANDING')
TABLE_FILE_CONTROL  = os.environ.get('DYNAMODB_TABLE_FILE_CONTROL', 'itx-file-control')
TABLE_FILE_PATTERN  = os.environ.get('DYNAMODB_TABLE_FILE_PATTERN', 'itx-file-pattern')
STEP_FUNCTION_ARN   = os.environ.get('STEP_FUNCTION_ARN')
UNZIP_FUNCTION_NAME = os.environ.get('UNZIP_FUNCTION_NAME', 'itx-unzip')

HASH_CHUNK_SIZE = 1 * 1024 * 1024


# =============================================================================
# DETECCIÓN Y DELEGACIÓN DE ZIPs
# =============================================================================

def _is_zip_file(filename: str) -> bool:
    """Detecta si el archivo es un ZIP por su extensión."""
    return filename.lower().endswith('.zip')


def _extraer_fecha_de_zip(filename: str) -> str:
    """
    Extrae la fecha del nombre del archivo ZIP.
    Soporta dos formatos presentes en los clientes:

      YYYYMMDD: 20260416visaout.zip → 2026-04-16
                20260416mcin.zip    → 2026-04-16
      YYMMDD:   MAST260416.zip      → 2026-04-16
                VISA260416.zip      → 2026-04-16

    Estrategia:
      1. Buscar YYYYMMDD primero (8 dígitos) — más específico
      2. Si no → buscar YYMMDD (6 dígitos)
      3. Si no → fecha actual como fallback
    """
    fecha_default = datetime.utcnow().strftime("%Y-%m-%d")

    # Intentar YYYYMMDD (8 dígitos consecutivos)
    match = re.search(r'(\d{8})', filename)
    if match:
        try:
            dt    = datetime.strptime(match.group(1), '%Y%m%d')
            fecha = dt.strftime('%Y-%m-%d')
            logger.info(f"  Fecha ZIP (YYYYMMDD): {match.group(1)} → {fecha}")
            return fecha
        except ValueError:
            pass  # no era fecha válida, seguir buscando

    # Intentar YYMMDD (6 dígitos consecutivos)
    match = re.search(r'(\d{6})', filename)
    if match:
        try:
            dt    = datetime.strptime(match.group(1), '%y%m%d')
            fecha = dt.strftime('%Y-%m-%d')
            logger.info(f"  Fecha ZIP (YYMMDD): {match.group(1)} → {fecha}")
            return fecha
        except ValueError:
            pass

    logger.warning(f"  No se pudo extraer fecha de '{filename}' → usando fecha actual")
    return fecha_default


def _handle_zip(
    bucket: str,
    key: str,
    client_id: str,
    file_date: str
) -> Dict:
    """
    Delega el procesamiento del ZIP a itx-unzip de forma asíncrona.

    Por qué asíncrono (InvocationType='Event'):
      - El router no espera que el unzip termine
      - El unzip puede tardar varios minutos (ZIPs de 1-2GB)
      - Los archivos extraídos dispararán el router nuevamente
        via S3 Event automáticamente → paralelismo gratis
    """
    payload = {
        'client_id':      client_id,
        'bucket_landing': bucket,
        's3_key':         key,
        'file_date':      file_date,
    }

    logger.info(f"  ZIP detectado → delegando a {UNZIP_FUNCTION_NAME} (async)")
    logger.info(f"  file_date extraída: {file_date}")

    lambda_client.invoke(
        FunctionName=UNZIP_FUNCTION_NAME,
        InvocationType='Event',
        Payload=json.dumps(payload).encode()
    )

    logger.info(f"  itx-unzip invocado — router continúa sin esperar")

    return {
        'file':      key.split('/')[-1],
        'status':    'DELEGATED_TO_UNZIP',
        'file_date': file_date,
    }


# =============================================================================
# IDENTIFICACIÓN DE ARCHIVOS
# =============================================================================

def generar_file_id(client_id: str, filename: str) -> str:
    """
    Genera un ID determinista basado en el nombre del archivo.
    Mismo archivo siempre produce el mismo ID → permite detectar duplicados.
    """
    match = re.search(r"(\d{8})", filename)
    fecha = match.group(1) if match else "NODATE"
    texto = f"{client_id}|{filename}|{fecha}"
    return hashlib.md5(texto.encode()).hexdigest().upper()


def generar_file_id_unico(client_id: str, filename: str, content_hash: str) -> str:
    """
    Genera un ID nuevo cuando llega el mismo archivo con contenido diferente.
    Incorpora el content_hash para garantizar unicidad.
    """
    texto = f"{client_id}|{filename}|{content_hash[:16]}"
    return hashlib.md5(texto.encode()).hexdigest().upper()


def calcular_content_hash(bucket: str, key: str) -> str:
    """
    Calcula el MD5 del archivo en streaming, sin cargarlo completo en RAM.

    Estrategia:
      1. Usar S3 ETag si el archivo fue subido en PUT simple (ETag = MD5)
      2. Si multipart → calcular MD5 en streaming leyendo chunks de 1MB
    """
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        etag = head.get('ETag', '').strip('"')

        if etag and '-' not in etag:
            logger.info(f"  content_hash: usando S3 ETag (no multipart)")
            return etag.upper()

        logger.info(f"  content_hash: streaming MD5 (multipart)")
        md5      = hashlib.md5()
        response = s3.get_object(Bucket=bucket, Key=key)
        body     = response['Body']

        while True:
            chunk = body.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            md5.update(chunk)

        return md5.hexdigest().upper()

    except Exception as e:
        logger.error(f"Error calculando content_hash de s3://{bucket}/{key}: {e}")
        return ""


def obtener_file_size(bucket: str, key: str, event_size: int = 0) -> int:
    if event_size > 0:
        return event_size
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except Exception as e:
        logger.warning(f"Error obteniendo size: {e}")
        return 0


# =============================================================================
# DETECCIÓN DE FECHA DEL ARCHIVO CTF
# =============================================================================

def convertir_fecha_juliana(texto_juliano: str) -> Optional[str]:
    """
    Convierte formato YYDDD a YYYY-MM-DD.
    YY = año (00-99), DDD = día del año (001-365).
    """
    if not texto_juliano or not texto_juliano.isdigit() or len(texto_juliano) != 5:
        return None
    try:
        dt = datetime.strptime(texto_juliano, "%y%j")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def extraer_fecha(bucket: str, key: str) -> str:
    """
    Extrae la fecha de procesamiento del header del archivo CTF.
    Lee solo los primeros 50 bytes (Range request).
    Solo aplica a archivos CTF — no a ZIPs.
    """
    fecha_default = datetime.utcnow().strftime("%Y-%m-%d")

    try:
        response = s3.get_object(Bucket=bucket, Key=key, Range='bytes=0-49')
        cabecera  = response['Body'].read().decode('latin-1')

        if len(cabecera) < 13:
            logger.warning("Header demasiado corto")
            return fecha_default

        for start, end in [(8, 13), (10, 15)]:
            texto_juliano = cabecera[start:end]
            fecha = convertir_fecha_juliana(texto_juliano)
            if fecha:
                logger.info(f"  Fecha detectada pos[{start}:{end}] ({texto_juliano}): {fecha}")
                return fecha

        logger.warning(f"No se pudo detectar fecha juliana. Raw header: {cabecera[:20]!r}")
        return fecha_default

    except Exception as e:
        logger.error(f"Error leyendo fecha: {e}")
        return fecha_default


# =============================================================================
# CLASIFICACIÓN DE ARCHIVOS
# =============================================================================

def cargar_patrones(customer_code: str = None) -> List[Dict]:
    """
    Carga patrones de clasificación desde DynamoDB.
    Filtra por customer_code o 'ALL', ordenados por prioridad.
    """
    try:
        table    = dynamodb.Table(TABLE_FILE_PATTERN)
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
            logger.warning("No hay patrones activos en DynamoDB")
            return []

        items.sort(key=lambda x: int(x.get('priority', 999)))

        if customer_code:
            items = [p for p in items if p.get('customer_code') in [customer_code, 'ALL']]

        logger.info(f"  {len(items)} patrones cargados para '{customer_code}'")
        return items

    except Exception as e:
        logger.error(f"Error cargando patrones: {e}")
        return []


def clasificar_archivo(filename: str, patrones: List[Dict]) -> Optional[Dict]:
    """
    Aplica los patrones regex en orden de prioridad.
    Retorna la clasificación del primer match, o None.
    """
    for patron in patrones:
        regex = patron.get("file_format", "")
        if not regex:
            continue
        try:
            if re.search(regex, filename, re.IGNORECASE):
                logger.info(f"  Match patrón: {patron.get('pattern_id')} ({regex[:50]})")
                return {
                    "brand":         patron.get("brand", "UNKNOWN"),
                    "direction":     patron.get("direction", "UNKNOWN"),
                    "file_type":     patron.get("file_type", "UNKNOWN"),
                    "customer_code": patron.get("customer_code"),
                    "pattern_id":    patron.get("pattern_id")
                }
        except re.error as e:
            logger.warning(f"  Regex inválido en patrón {patron.get('pattern_id')}: {e}")

    return None


# =============================================================================
# CONTROL DE DUPLICADOS
# =============================================================================

def verificar_duplicado(file_id: str, content_hash: str) -> Tuple[str, Optional[str]]:
    """
    Returns:
      ("nuevo", None)            → nunca visto
      ("duplicado", file_id)     → mismo nombre Y mismo contenido → ignorar
      ("version_nueva", file_id) → mismo nombre, distinto contenido → reprocesar
    """
    try:
        table    = dynamodb.Table(TABLE_FILE_CONTROL)
        response = table.get_item(Key={'file_id': file_id})

        if 'Item' not in response:
            return ("nuevo", None)

        hash_existente = response['Item'].get('content_hash', '')

        if hash_existente == content_hash:
            return ("duplicado", file_id)
        else:
            return ("version_nueva", file_id)

    except Exception as e:
        logger.warning(f"Error verificando duplicado: {e}")
        return ("nuevo", None)


# =============================================================================
# REGISTRO EN DYNAMODB
# =============================================================================

def registrar_archivo(
    file_id: str, client_id: str, filename: str,
    bucket: str, s3_key: str, file_size: int,
    content_hash: str, clasificacion: Dict, file_date: str
) -> bool:
    try:
        table     = dynamodb.Table(TABLE_FILE_CONTROL)
        direction = clasificacion['direction'].upper()
        file_type = 'IN' if direction in ['IN', 'INCOMING'] else 'OUT'
        brand_id  = 'VI' if clasificacion['brand'].upper() == 'VISA' else 'MC'

        registro = {
            'file_id':              file_id,
            'client_id':            client_id,
            'landing_file_name':    filename,
            'file_path':            f"s3://{bucket}/{s3_key}",
            'file_size':            file_size,
            'content_hash':         content_hash,
            'brand_id':             brand_id,
            'file_type':            file_type,
            'file_processing_date': file_date,
            'detected_at':          datetime.utcnow().isoformat(),
            'control_status':       'PENDING',
            'process_start_ts':     None,
            'process_finish_ts':    None,
            'error_message':        None,
        }

        table.put_item(Item=registro)
        logger.info(f"  Archivo registrado → file_id: {file_id}")
        return True

    except Exception as e:
        logger.error(f"Error registrando archivo en DynamoDB: {e}")
        return False


def actualizar_estado(file_id: str, estado: str, error: str = None):
    try:
        table   = dynamodb.Table(TABLE_FILE_CONTROL)
        now     = datetime.utcnow().isoformat()
        estado  = estado.upper()

        update_expr = "SET control_status = :status"
        expr_values = {':status': estado}

        if estado == 'PROCESSING':
            update_expr += ", process_start_ts = :ts"
            expr_values[':ts'] = now
        elif estado in ('COMPLETED', 'FAILED'):
            update_expr += ", process_finish_ts = :ts"
            expr_values[':ts'] = now

        if error:
            update_expr += ", error_message = :err"
            expr_values[':err'] = str(error)[:500]

        table.update_item(
            Key={'file_id': file_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values
        )
        logger.info(f"  Estado → {estado} (file_id: {file_id})")

    except Exception as e:
        logger.error(f"Error actualizando estado: {e}")


# =============================================================================
# INICIO DE STEP FUNCTIONS
# =============================================================================

def iniciar_step_function(
    client_id: str, file_id: str, filename: str,
    bucket: str, s3_key: str, clasificacion: Dict,
    file_date: str, content_hash: str
) -> str:
    direction = clasificacion['direction'].upper()
    file_type = 'IN' if direction in ['IN', 'INCOMING'] else 'OUT'
    brand_id  = 'VI' if clasificacion['brand'].upper() == 'VISA' else 'MC'

    sfn_input = {
        'client_id':      client_id,
        'file_id':        file_id,
        'filename':       filename,
        's3_key_landing': s3_key,
        'bucket_landing': bucket,
        'brand':          clasificacion['brand'],
        'brand_id':       brand_id,
        'file_type':      file_type,
        'file_date':      file_date,
        'content_hash':   content_hash,
    }

    execution_name = (
        f"{client_id}-{file_id[:8]}-"
        f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    )

    response = sfn.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        name=execution_name,
        input=json.dumps(sfn_input)
    )

    execution_arn = response['executionArn']
    logger.info(f"  Step Function iniciado: {execution_arn}")
    return execution_arn


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Procesa eventos S3 de llegada de archivos a Landing.
    Un evento puede contener múltiples records (batch de S3).
    """
    logger.info("=== ITX Router Lambda ===")
    logger.info(f"Event: {json.dumps(event)}")

    if not STEP_FUNCTION_ARN:
        raise ValueError("Falta variable de entorno: STEP_FUNCTION_ARN")

    results = []

    for record in event.get('Records', []):
        bucket   = None
        key      = None
        filename = "unknown"

        try:
            bucket     = record['s3']['bucket']['name']
            key        = unquote_plus(record['s3']['object']['key'])
            event_size = record['s3']['object'].get('size', 0)

            logger.info(f"--- Procesando: s3://{bucket}/{key} ({event_size:,} bytes) ---")

            parts = key.split('/')
            if len(parts) < 2:
                logger.error(f"Path inválido: {key}")
                results.append({'file': key, 'status': 'ERROR', 'error': 'Invalid path'})
                continue

            client_id = parts[0]
            filename  = parts[-1]

            if not filename or filename.startswith('.'):
                logger.info(f"Ignorando: {key}")
                continue

            logger.info(f"  Client: {client_id}, File: {filename}")

            # ── Detectar ZIP → delegar a itx-unzip ───────────────────────
            if _is_zip_file(filename):
                file_date  = _extraer_fecha_de_zip(filename)
                zip_result = _handle_zip(bucket, key, client_id, file_date)
                results.append(zip_result)
                continue
            # ─────────────────────────────────────────────────────────────

            # Cargar patrones
            patrones = cargar_patrones(client_id)
            if not patrones:
                msg = f"No hay patrones activos para '{client_id}'"
                logger.error(msg)
                results.append({'file': filename, 'status': 'ERROR', 'error': msg})
                continue

            # Clasificar
            clasificacion = clasificar_archivo(filename, patrones)
            if not clasificacion:
                logger.warning(f"  Sin match de patrón: {filename}")
                results.append({'file': filename, 'status': 'SKIPPED', 'reason': 'No pattern match'})
                continue

            logger.info(f"  Clasificado: {clasificacion['brand']} / {clasificacion['direction']}")

            # Generar file_id
            file_id = generar_file_id(client_id, filename)

            # Calcular content_hash en streaming
            content_hash = calcular_content_hash(bucket, key)
            if not content_hash:
                logger.warning("  content_hash vacío → usando file_id como fallback")
                content_hash = file_id

            # Extraer fecha del header CTF (50 bytes)
            file_date = extraer_fecha(bucket, key)
            file_size = obtener_file_size(bucket, key, event_size)

            logger.info(f"  file_id: {file_id[:16]}... | date: {file_date} | size: {file_size:,}B")

            # Verificar duplicado
            estado_dup, _ = verificar_duplicado(file_id, content_hash)

            if estado_dup == "duplicado":
                logger.info(f"  DUPLICADO — ya procesado: {file_id}")
                results.append({'file': filename, 'status': 'SKIPPED', 'reason': 'Duplicate'})
                continue

            elif estado_dup == "version_nueva":
                logger.info("  VERSION NUEVA — generando nuevo file_id")
                file_id = generar_file_id_unico(client_id, filename, content_hash)
                logger.info(f"  Nuevo file_id: {file_id[:16]}...")

            # Registrar en DynamoDB
            if not registrar_archivo(
                file_id=file_id, client_id=client_id, filename=filename,
                bucket=bucket, s3_key=key, file_size=file_size,
                content_hash=content_hash, clasificacion=clasificacion,
                file_date=file_date
            ):
                logger.error("  Falló registro en DynamoDB")
                results.append({'file': filename, 'status': 'ERROR', 'error': 'DynamoDB failed'})
                continue

            # Iniciar Step Functions
            actualizar_estado(file_id, 'PROCESSING')

            try:
                execution_arn = iniciar_step_function(
                    client_id=client_id, file_id=file_id, filename=filename,
                    bucket=bucket, s3_key=key, clasificacion=clasificacion,
                    file_date=file_date, content_hash=content_hash
                )
                logger.info(f"  Procesamiento iniciado: {execution_arn}")
                results.append({
                    'file':          filename,
                    'status':        'STARTED',
                    'file_id':       file_id,
                    'execution_arn': execution_arn
                })

            except Exception as e:
                logger.error(f"  Error iniciando Step Functions: {e}")
                actualizar_estado(file_id, 'FAILED', str(e))
                results.append({'file': filename, 'status': 'ERROR', 'error': str(e)})
                continue  # no raise — procesar los demás records del batch

        except Exception as e:
            logger.error(f"Error procesando record: {e}", exc_info=True)
            results.append({'file': filename, 'status': 'ERROR', 'error': str(e)})
            continue

    logger.info("=== Router Complete ===")
    logger.info(f"Results: {json.dumps(results)}")

    return {
        'statusCode': 200,
        'body': json.dumps({'results': results})
    }
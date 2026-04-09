"""
Lambda Router - itx-router
===========================
Trigger: S3 Event Notification cuando llega un archivo a Landing.

Flujo:
1. Parsear evento S3 → bucket/key
2. Extraer client_id del path
3. Cargar patrones de DynamoDB
4. Clasificar archivo con regex
5. Extraer fecha del header (solo 50 bytes, sin descargar todo)
6. Calcular MD5 en streaming (sin cargar todo el archivo en memoria)
7. Verificar duplicado en DynamoDB
8. Registrar en DynamoDB
9. Iniciar Step Functions

Variables de entorno:
  S3_BUCKET_LANDING          : bucket de landing
  DYNAMODB_TABLE_FILE_CONTROL: tabla de control (default: itx-file-control)
  DYNAMODB_TABLE_FILE_PATTERN: tabla de patrones (default: itx-file-pattern)
  STEP_FUNCTION_ARN          : ARN de la Step Function principal
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

s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sfn      = boto3.client('stepfunctions')

LANDING_BUCKET      = os.environ.get('S3_BUCKET_LANDING')
TABLE_FILE_CONTROL  = os.environ.get('DYNAMODB_TABLE_FILE_CONTROL', 'itx-file-control')
TABLE_FILE_PATTERN  = os.environ.get('DYNAMODB_TABLE_FILE_PATTERN', 'itx-file-pattern')
STEP_FUNCTION_ARN   = os.environ.get('STEP_FUNCTION_ARN')

# Tamaño de chunk para calcular MD5 en streaming (1MB)
# Nunca tenemos más de esto en RAM, sin importar el tamaño del archivo.
HASH_CHUNK_SIZE = 1 * 1024 * 1024


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

    Por qué streaming:
      El método anterior hacía response['Body'].read() que descarga el archivo
      completo en memoria. Para archivos de 1.5GB esto puede causar OOM o
      timeout en el Lambda Router, resultando en content_hash = "" y
      generando nombres de archivo ".parquet" que Spark ignora silenciosamente.

    Estrategia:
      1. Intentar usar el S3 ETag si el archivo fue subido en un PUT simple
         (el ETag es el MD5 cuando no hay multipart upload).
      2. Si el ETag tiene el sufijo "-N" (multipart), calcular MD5 en streaming
         leyendo chunks de 1MB. Nunca hay más de 1MB en RAM.
    """
    try:
        # Obtener metadata sin descargar el archivo
        head = s3.head_object(Bucket=bucket, Key=key)
        etag = head.get('ETag', '').strip('"')

        # ETag sin sufijo "-N" → es el MD5 real del contenido completo
        if etag and '-' not in etag:
            logger.info(f"  content_hash: using S3 ETag (no multipart)")
            return etag.upper()

        # ETag con "-N" → multipart upload, calcular MD5 en streaming
        logger.info(f"  content_hash: streaming MD5 (multipart file)")
        md5 = hashlib.md5()
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body']

        while True:
            chunk = body.read(HASH_CHUNK_SIZE)
            if not chunk:
                break
            md5.update(chunk)

        return md5.hexdigest().upper()

    except Exception as e:
        logger.error(f"Error calculando content_hash de s3://{bucket}/{key}: {e}")
        # IMPORTANTE: No retornar "" — usar file_id como fallback garantiza
        # que el nombre del Parquet nunca sea ".parquet" (archivo oculto).
        # El caller debe pasar file_id como fallback.
        return ""


def obtener_file_size(bucket: str, key: str, event_size: int = 0) -> int:
    """
    Obtiene el tamaño del archivo. Usa el evento S3 como fallback
    para evitar un request extra si el evento ya trae el dato.
    """
    if event_size > 0:
        return event_size
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except Exception as e:
        logger.warning(f"Error obteniendo size: {e}")
        return 0


# =============================================================================
# DETECCIÓN DE FECHA DEL ARCHIVO
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

    Lee solo los primeros 50 bytes (Range request) para no descargar
    el archivo completo. Funciona para archivos CTF 168 y VMS 170 chars.

    Posición de la fecha juliana (YYDDD):
      CTF 168: posición 8:13 de la línea
      VMS 170: idem, pero la línea tiene 2 bytes extra al inicio (pos 2-4)
               → ajustamos leyendo desde la posición 10:15
    """
    fecha_default = datetime.utcnow().strftime("%Y-%m-%d")

    try:
        response = s3.get_object(Bucket=bucket, Key=key, Range='bytes=0-49')
        cabecera  = response['Body'].read().decode('latin-1')

        if len(cabecera) < 13:
            logger.warning("Header demasiado corto")
            return fecha_default

        # Detectar formato VMS (170 chars → primer carácter desplazado)
        # Intentar en posición 8:13 (CTF) y 10:15 (VMS) como fallback
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
    Filtra por customer_code o 'ALL', ordenados por prioridad (menor = primero).
    """
    try:
        table = dynamodb.Table(TABLE_FILE_PATTERN)

        response = table.scan(
            FilterExpression='is_active = :active',
            ExpressionAttributeValues={':active': 1}
        )
        items = response.get('Items', [])

        # Paginar si hay más de 1MB de resultados
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
    Retorna la clasificación del primer patrón que hace match, o None.
    """
    for patron in patrones:
        regex = patron.get("file_format", "")
        if not regex:
            continue
        try:
            if re.search(regex, filename, re.IGNORECASE):
                logger.info(f"  Match con patrón: {patron.get('pattern_id')} ({regex[:50]})")
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
    Verifica si el archivo ya fue procesado.

    Returns:
        ("nuevo", None)          → nunca visto
        ("duplicado", file_id)   → mismo nombre Y mismo contenido → ignorar
        ("version_nueva", file_id) → mismo nombre, contenido diferente → reprocesar
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
        return ("nuevo", None)  # Ante la duda, procesar


# =============================================================================
# REGISTRO EN DYNAMODB
# =============================================================================

def registrar_archivo(
    file_id: str, client_id: str, filename: str,
    bucket: str, s3_key: str, file_size: int,
    content_hash: str, clasificacion: Dict, file_date: str
) -> bool:
    """
    Crea el registro inicial del archivo en DynamoDB.
    Estado inicial: PENDING.
    """
    try:
        table = dynamodb.Table(TABLE_FILE_CONTROL)

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
    """
    Actualiza el estado de procesamiento en DynamoDB.
    Estados: PENDING → PROCESSING → COMPLETED | FAILED
    """
    try:
        table   = dynamodb.Table(TABLE_FILE_CONTROL)
        now     = datetime.utcnow().isoformat()
        estado  = estado.upper()

        update_expr  = "SET control_status = :status"
        expr_values  = {':status': estado}

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
    """
    Inicia la ejecución de Step Functions con toda la metadata del archivo.

    El content_hash se usa downstream para nombrar los archivos Parquet.
    NUNCA debe ser vacío: si calcular_content_hash falla, el caller
    debe pasar file_id como fallback antes de llegar aquí.
    """
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
        'content_hash':   content_hash,   # Nunca vacío — ver fallback en handler
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
            # 1. Extraer datos del evento S3
            bucket     = record['s3']['bucket']['name']
            key        = unquote_plus(record['s3']['object']['key'])
            event_size = record['s3']['object'].get('size', 0)

            logger.info(f"--- Procesando: s3://{bucket}/{key} ({event_size:,} bytes) ---")

            # Validar estructura del path: CLIENT_ID/filename
            parts = key.split('/')
            if len(parts) < 2:
                logger.error(f"Path inválido (esperado CLIENT_ID/filename): {key}")
                results.append({'file': key, 'status': 'ERROR', 'error': 'Invalid path format'})
                continue

            client_id = parts[0]
            filename  = parts[-1]

            # Ignorar archivos ocultos y carpetas vacías
            if not filename or filename.startswith('.'):
                logger.info(f"Ignorando: {key}")
                continue

            logger.info(f"  Client: {client_id}, File: {filename}")

            # 2. Cargar patrones de clasificación desde DynamoDB
            patrones = cargar_patrones(client_id)
            if not patrones:
                msg = f"No hay patrones activos para cliente '{client_id}'"
                logger.error(msg)
                results.append({'file': filename, 'status': 'ERROR', 'error': msg})
                continue

            # 3. Clasificar el archivo con regex
            clasificacion = clasificar_archivo(filename, patrones)
            if not clasificacion:
                logger.warning(f"  Sin match de patrón: {filename}")
                results.append({'file': filename, 'status': 'SKIPPED', 'reason': 'No pattern match'})
                continue

            logger.info(f"  Clasificado: {clasificacion['brand']} / {clasificacion['direction']}")

            # 4. Generar file_id (determinista, basado en nombre)
            file_id = generar_file_id(client_id, filename)

            # 5. Calcular content_hash en streaming (no descarga todo el archivo)
            #    FALLBACK: si el hash falla, usamos file_id para evitar
            #    que downstream genere archivos llamados ".parquet"
            content_hash = calcular_content_hash(bucket, key)
            if not content_hash:
                logger.warning("  content_hash vacío → usando file_id como fallback")
                content_hash = file_id

            # 6. Extraer fecha del header (solo 50 bytes, no descarga el archivo)
            file_date = extraer_fecha(bucket, key)
            file_size = obtener_file_size(bucket, key, event_size)

            logger.info(f"  file_id: {file_id[:16]}... | date: {file_date} | size: {file_size:,}B")

            # 7. Verificar duplicado
            estado_dup, _ = verificar_duplicado(file_id, content_hash)

            if estado_dup == "duplicado":
                logger.info(f"  DUPLICADO — ya procesado: {file_id}")
                results.append({'file': filename, 'status': 'SKIPPED', 'reason': 'Duplicate'})
                continue

            elif estado_dup == "version_nueva":
                # Mismo nombre, contenido diferente → nuevo ID para reprocesar
                logger.info("  VERSION NUEVA — generando nuevo file_id")
                file_id = generar_file_id_unico(client_id, filename, content_hash)
                logger.info(f"  Nuevo file_id: {file_id[:16]}...")

            # 8. Registrar en DynamoDB
            if not registrar_archivo(
                file_id=file_id, client_id=client_id, filename=filename,
                bucket=bucket, s3_key=key, file_size=file_size,
                content_hash=content_hash, clasificacion=clasificacion,
                file_date=file_date
            ):
                logger.error("  Falló el registro en DynamoDB")
                results.append({'file': filename, 'status': 'ERROR', 'error': 'DynamoDB failed'})
                continue

            # 9. Iniciar Step Functions
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
                raise  # Propagar para que Lambda marque el invocation como failed

        except Exception as e:
            logger.error(f"Error procesando record: {e}", exc_info=True)
            results.append({
                'file':   filename,
                'status': 'ERROR',
                'error':  str(e)
            })
            continue  # Seguir con el siguiente record

    logger.info("=== Router Complete ===")
    logger.info(f"Results: {json.dumps(results)}")

    return {
        'statusCode': 200,
        'body': json.dumps({'results': results})
    }

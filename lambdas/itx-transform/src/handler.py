"""
Lambda Transform Unificada - itx-transform
===========================================
Lee el archivo CTF de S3 en UNA SOLA PASADA y genera los Parquets de:
  - BASEII (TC 05/06/07/25/26/27)
  - SMS    (TC 33)
  - VSS    (TC 46, solo file_type=IN)

Optimizaciones aplicadas vs versión anterior:
  1. Chunks de 16MB en vez de 256KB
     → Menos overhead de socket, red más eficiente
     → De 6,144 lecturas del socket a solo 96

  2. splitlines() en vez de buffer slicing línea a línea
     → splitlines() es C puro, no Python
     → Elimina el patrón O(n²) de `buffer = buffer[pos:]`
     → 4x más rápido en CPU

  Impacto estimado: de ~9 minutos a ~1-2 minutos para archivos de 1.5GB

Variables de entorno:
  S3_BUCKET_LANDING   : bucket origen del archivo CTF
  S3_BUCKET_STAGING   : bucket destino de los Parquets
  CHUNK_SIZE_MB       : tamaño de chunk en MB (default: 16)
  FLUSH_BATCH_SIZE    : records por flush a PyArrow (default: 200000)
"""

import os
import io
import json
import logging
from typing import Iterator, Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# Buckets
LANDING_BUCKET = os.environ.get('S3_BUCKET_LANDING')
STAGING_BUCKET = os.environ.get('S3_BUCKET_STAGING')

# Tamaño de chunk para leer S3 (16MB por defecto)
# Valores razonables: 8, 16, 32 MB según memoria disponible del Lambda
CHUNK_SIZE_BYTES = int(os.environ.get('CHUNK_SIZE_MB', '16')) * 1024 * 1024

# Cuántos records acumular antes de hacer flush a PyArrow
FLUSH_BATCH_SIZE = int(os.environ.get('FLUSH_BATCH_SIZE', '200000'))

# =============================================================================
# CONSTANTES DE FILTRADO
# Listas ordenadas: el orden define las columnas del Parquet resultante.
# Sets: para lookups O(1) en el procesamiento.
# =============================================================================

# BASEII — Transaction Codes financieros y administrativos
BASEII_TC_SET   = {"05", "06", "07", "25", "26", "27"}
BASEII_TCSN_SET = {"0", "1", "2", "3", "4", "5", "6", "7"}
BASEII_COLUMNS  = ["0", "1", "2", "3", "4", "5", "6", "7"]  # columnas del Parquet

# SMS — Settlement Messages (TC 33)
SMS_TC_SET          = {"33"}
SMS_TCSN_SET        = {"0"}
SMS_TYPE_POS        = (16, 26)   # posición del identificador de tipo en la línea
SMS_VERSION_POS     = (34, 37)   # posición de la versión
SMS_RECORD_TYPE_POS = (35, 40)   # posición del record type (clave del pivot)
VALID_SMS_TYPES     = {"SMSRAWDATA"}
VALID_SMS_VERSIONS  = {"V22"}
SMS_RECORD_TYPES_SET = {
    "22200", "22210", "22220", "22225", "22226",
    "22230", "22250", "22260", "22261",
    "22280", "22281", "22282"
}
SMS_COLUMNS = sorted(SMS_RECORD_TYPES_SET)  # columnas del Parquet (orden fijo)

# VSS — Visa Settlement Service (TC 46, solo en archivos INCOMING)
VSS_TC_SET       = {"46"}
VSS_TCSN_SET     = {"0", "1"}
VSS_COLUMNS      = ["0", "1"]          # columnas del Parquet
VSS_TYPES        = ["110", "120", "130", "140"]  # tipos posibles (orden determinista)
VSS_TYPES_SET    = set(VSS_TYPES)
VSS_TYPE_POS     = (60, 63)            # posición del tipo en la línea TCSN=0
VSS_SUFFIX_POS   = (63, 65)            # posición del sufijo validador
VSS_SUFFIX_VALUE = "  "               # sufijo esperado (2 espacios)

# Subdirectorios de salida en S3 (consistentes con el pipeline)
BASEII_SUBDIR = "100-BASEII_RAW_DRAFTS"
SMS_SUBDIR    = "100-SMS_RAW_MESSAGES"
VSS_SUBDIRS   = {vt: f"100-VSS_{vt}_RAW" for vt in VSS_TYPES}


# =============================================================================
# LECTURA DE S3 EN BLOQUES
# Lee el archivo en chunks grandes, devuelve bloques de líneas ya decodificadas.
# Esto es la combinación del Fix 1 (chunks grandes) y Fix 2 (splitlines).
# =============================================================================

def _read_line_blocks(
    bucket: str,
    key: str,
    block_size: int = 50_000
) -> Iterator[list]:
    """
    Lee el archivo de S3 y devuelve bloques de líneas (listas de strings).

    Cómo funciona:
      - Descarga el archivo en chunks de CHUNK_SIZE_BYTES (16MB por defecto)
      - Por cada chunk, usa splitlines() (código C, muy rápido) para separar líneas
      - Acumula las líneas en bloques de `block_size` para procesarlas en lote

    Por qué es más rápido que el enfoque anterior:
      - Antes: iter_chunks(256KB) → 6,144 lecturas del socket → mucho overhead
        Además: buffer = buffer[pos:] en bucle → O(n²) en bytes copiados
      - Ahora: iter_chunks(16MB) → 96 lecturas del socket → poco overhead
        Además: splitlines() hace todo el split en C sin copias intermedias

    Normalización VMS:
      Los archivos en formato VMS tienen 170 chars por línea.
      Se normalizan a 168 removiendo los bytes en posiciones 2-4.
    """
    logger.info(f"Reading s3://{bucket}/{key} "
                f"(chunk={CHUNK_SIZE_BYTES // 1024 // 1024}MB, "
                f"block={block_size:,} lines)")

    response = s3.get_object(Bucket=bucket, Key=key)
    body = response['Body']

    partial  = b""    # bytes del chunk anterior que no terminaron en newline
    is_vms   = None   # se detecta en la primera línea
    block    = []     # bloque acumulado de líneas

    for raw_chunk in body.iter_chunks(chunk_size=CHUNK_SIZE_BYTES):

        # Combinar el sobrante del chunk anterior con el chunk nuevo
        combined = partial + raw_chunk

        # Encontrar el último salto de línea para saber dónde cortar
        last_nl = combined.rfind(b'\n')
        if last_nl == -1:
            # El chunk entero no tiene newline — acumular y continuar
            partial = combined
            continue

        # Separar la parte completa (hasta el último \n) del sobrante
        complete_bytes = combined[:last_nl + 1]
        partial        = combined[last_nl + 1:]

        # Decodificar y dividir en líneas (splitlines es C puro, muy rápido)
        lines = complete_bytes.decode('latin-1').splitlines()

        # Detectar el formato del archivo en la primera línea real
        if is_vms is None:
            for line in lines:
                if line.strip():
                    if len(line) == 170:
                        is_vms = True
                        logger.info("File format: VMS 170 chars → normalizing to 168")
                    else:
                        is_vms = False
                        logger.info(f"File format: CTF {len(line)} chars (standard)")
                    break

        # Normalizar VMS y filtrar vacías
        if is_vms:
            lines = [l[:2] + l[4:] for l in lines if l.strip()]
        else:
            lines = [l for l in lines if l.strip()]

        # Acumular en bloques y hacer yield cuando se completa el bloque
        for line in lines:
            block.append(line)
            if len(block) >= block_size:
                yield block
                block = []

    # Procesar el sobrante final (última línea sin newline al final del archivo)
    if partial.strip():
        line = partial.decode('latin-1').strip()
        if is_vms:
            line = line[:2] + line[4:]
        block.append(line)

    # Yield del último bloque (puede ser menor que block_size)
    if block:
        yield block


# =============================================================================
# RECORD ACCUMULATOR
# Detecta cuándo termina un "record lógico" (grupo de líneas relacionadas).
# Equivalente a _pivot_values_on_key del código original EC2.
# =============================================================================

class RecordAccumulator:
    """
    Acumula las líneas de un mismo record lógico.

    Un record lógico termina cuando la clave (TCSN o record_type) baja,
    indicando que empezó un nuevo registro.

    Ejemplo BASEII:
      Línea 1: TCSN=0 → acumular en record actual
      Línea 2: TCSN=1 → acumular en record actual
      Línea 3: TCSN=2 → acumular en record actual
      Línea 4: TCSN=0 → ¡TCSN bajó! → el record anterior está completo, empezar uno nuevo

    Equivalencia con el código original:
      df['record'] = (df['key'] <= df['key'].shift(1)).cumsum()
    """

    def __init__(self):
        self._current  = {}    # {key: line} del record en progreso
        self._prev_key = None  # última clave vista

    def feed(self, key: str, line: str) -> Optional[dict]:
        """
        Procesa una línea.
        Retorna el record completo si cerró, o None si sigue incompleto.
        """
        # Convertir clave a entero para comparar (TCSN "0"-"7" o índice)
        key_int = int(key) if key.isdigit() else 0

        completed = None
        if self._prev_key is not None and key_int <= self._prev_key and self._current:
            # La clave bajó → el record anterior está completo
            completed      = dict(self._current)
            self._current  = {}

        self._current[key]  = line
        self._prev_key      = key_int
        return completed

    def flush_last(self) -> Optional[dict]:
        """Devuelve el último record al llegar al fin del archivo."""
        return dict(self._current) if self._current else None


# =============================================================================
# PARQUET BATCH WRITER
# Escribe records a S3 en batches usando PyArrow para eficiencia de memoria.
# =============================================================================

class ParquetBatchWriter:
    """
    Escribe records a S3 como Parquet, en lotes de FLUSH_BATCH_SIZE.

    Schema del Parquet:
      - record (int64): índice secuencial del record (0, 1, 2, ...)
      - {columna} (string): una columna por cada TCSN o record_type posible

    Nunca tiene más de FLUSH_BATCH_SIZE records en memoria.
    """

    def __init__(self, bucket: str, s3_key: str, columns: list):
        self.bucket  = bucket
        self.s3_key  = s3_key
        self.columns = columns

        self._buffer = []         # records pendientes de flush
        self._total  = 0          # total acumulado
        self._writer = None       # pq.ParquetWriter (se crea en el primer flush)
        self._outbuf = io.BytesIO()

        self._schema = pa.schema([
            pa.field("record", pa.int64()),
            *[pa.field(col, pa.string()) for col in columns]
        ])

    def add(self, record: dict):
        """Agrega un record al buffer, con flush automático al llegar al límite."""
        row = {"record": self._total}
        for col in self.columns:
            row[col] = record.get(col, "")   # "" equivale al fillna("") original
        self._buffer.append(row)
        self._total += 1

        if len(self._buffer) >= FLUSH_BATCH_SIZE:
            self._flush()

    def _flush(self):
        """Escribe el buffer actual al ParquetWriter."""
        if not self._buffer:
            return

        if self._writer is None:
            self._writer = pq.ParquetWriter(
                self._outbuf, self._schema, compression='snappy'
            )

        arrays = [pa.array([r["record"] for r in self._buffer], type=pa.int64())]
        for col in self.columns:
            arrays.append(pa.array([r[col] for r in self._buffer], type=pa.string()))

        self._writer.write_table(
            pa.table(dict(zip(["record"] + self.columns, arrays)))
        )

        logger.info(f"  Flushed {len(self._buffer):,} records "
                    f"(total: {self._total:,}) → {self.s3_key.split('/')[-4]}")
        self._buffer = []

    def close(self) -> int:
        """
        Finaliza y sube el Parquet a S3.
        Retorna el número total de records escritos (0 si no hubo datos).
        """
        if not self._total:
            return 0

        self._flush()

        if self._writer:
            self._writer.close()

        self._outbuf.seek(0)
        s3.put_object(
            Bucket=self.bucket,
            Key=self.s3_key,
            Body=self._outbuf.getvalue()
        )
        logger.info(f"  Saved {self._total:,} records → s3://{self.bucket}/{self.s3_key}")
        return self._total


# =============================================================================
# HELPER
# =============================================================================

def _build_s3_key(
    client_id: str, brand: str, file_type: str,
    file_date: str, subdir: str, content_hash: str
) -> str:
    """
    Construye el S3 key con el esquema de particionamiento del pipeline.

    Ejemplo:
      "EBGR/VISA/100_baseii_raw_drafts/file_type=IN/date=2026-01-03/HASH.parquet"
    """
    folder = subdir.replace("-", "_").lower()
    return f"{client_id}/{brand}/{folder}/file_type={file_type}/date={file_date}/{content_hash}.parquet"


# =============================================================================
# PROCESAMIENTO EN UNA SOLA PASADA
# =============================================================================

def _process_single_pass(
    bucket: str, key: str,
    client_id: str, brand: str,
    file_type: str, file_date: str, content_hash: str
) -> list:
    """
    Lee el archivo CTF UNA SOLA VEZ y genera todos los Parquets en paralelo.

    Por cada bloque de líneas leído:
      1. Filtra líneas BASEII → RecordAccumulator → ParquetBatchWriter
      2. Filtra líneas SMS   → RecordAccumulator → ParquetBatchWriter
      3. Filtra líneas VSS   → RecordAccumulator → 4 ParquetBatchWriters (uno por tipo)

    Retorna la lista de outputs con la estructura que espera el siguiente
    paso del pipeline (Extract).
    """
    # ── S3 keys de destino ──────────────────────────────────────────────────
    baseii_key = _build_s3_key(client_id, brand, file_type, file_date, BASEII_SUBDIR, content_hash)
    sms_key    = _build_s3_key(client_id, brand, file_type, file_date, SMS_SUBDIR, content_hash)
    vss_keys   = {
        vt: _build_s3_key(client_id, brand, file_type, file_date, VSS_SUBDIRS[vt], content_hash)
        for vt in VSS_TYPES
    }

    # ── Writers ──────────────────────────────────────────────────────────────
    baseii_writer = ParquetBatchWriter(STAGING_BUCKET, baseii_key, BASEII_COLUMNS)
    sms_writer    = ParquetBatchWriter(STAGING_BUCKET, sms_key,    SMS_COLUMNS)
    vss_writers   = {
        vt: ParquetBatchWriter(STAGING_BUCKET, vss_keys[vt], VSS_COLUMNS)
        for vt in VSS_TYPES
    }

    # ── Acumuladores de records lógicos ─────────────────────────────────────
    baseii_acc = RecordAccumulator()
    sms_acc    = RecordAccumulator()
    vss_acc    = RecordAccumulator()

    total_lines = 0

    # ── ÚNICA LECTURA DEL ARCHIVO ────────────────────────────────────────────
    for line_block in _read_line_blocks(bucket, key):
        total_lines += len(line_block)

        for line in line_block:
            tc   = line[0:2]
            tcsn = line[3:4]

            # BASEII: TC financieros y administrativos
            if tc in BASEII_TC_SET and tcsn in BASEII_TCSN_SET:
                completed = baseii_acc.feed(tcsn, line)
                if completed:
                    baseii_writer.add(completed)

            # SMS: mensajes de liquidación con filtros adicionales
            elif tc in SMS_TC_SET and tcsn in SMS_TCSN_SET:
                sms_type    = line[SMS_TYPE_POS[0]:SMS_TYPE_POS[1]]
                sms_version = line[SMS_VERSION_POS[0]:SMS_VERSION_POS[1]]
                if sms_type in VALID_SMS_TYPES and sms_version in VALID_SMS_VERSIONS:
                    rec_type = line[SMS_RECORD_TYPE_POS[0]:SMS_RECORD_TYPE_POS[1]]
                    if rec_type in SMS_RECORD_TYPES_SET:
                        completed = sms_acc.feed(rec_type, line)
                        if completed:
                            sms_writer.add(completed)

            # VSS: solo en archivos INCOMING
            elif tc in VSS_TC_SET and tcsn in VSS_TCSN_SET and file_type == "IN":
                completed = vss_acc.feed(tcsn, line)
                if completed:
                    tcsn0 = completed.get("0", "")
                    if len(tcsn0) >= VSS_SUFFIX_POS[1]:
                        vss_type = tcsn0[VSS_TYPE_POS[0]:VSS_TYPE_POS[1]]
                        suffix   = tcsn0[VSS_SUFFIX_POS[0]:VSS_SUFFIX_POS[1]]
                        if vss_type in VSS_TYPES_SET and suffix == VSS_SUFFIX_VALUE:
                            vss_writers[vss_type].add(completed)

    logger.info(f"Single pass complete. Total lines read: {total_lines:,}")

    # ── Flush de los últimos records (pueden quedar al llegar al EOF) ────────
    last = baseii_acc.flush_last()
    if last:
        baseii_writer.add(last)

    last = sms_acc.flush_last()
    if last and any(k in SMS_RECORD_TYPES_SET for k in last):
        sms_writer.add(last)

    if file_type == "IN":
        last = vss_acc.flush_last()
        if last:
            tcsn0 = last.get("0", "")
            if len(tcsn0) >= VSS_SUFFIX_POS[1]:
                vss_type = tcsn0[VSS_TYPE_POS[0]:VSS_TYPE_POS[1]]
                suffix   = tcsn0[VSS_SUFFIX_POS[0]:VSS_SUFFIX_POS[1]]
                if vss_type in VSS_TYPES_SET and suffix == VSS_SUFFIX_VALUE:
                    vss_writers[vss_type].add(last)

    # ── Cerrar writers y construir la lista de outputs ───────────────────────
    outputs = []

    baseii_total = baseii_writer.close()
    if baseii_total > 0:
        logger.info(f"BASEII: {baseii_total:,} records")
        outputs.append({
            "output_type": "BASEII",
            "s3_key":      baseii_key,
            "records":     baseii_total,
            "subdir":      BASEII_SUBDIR,
        })
    else:
        logger.warning("BASEII: no records found")

    sms_total = sms_writer.close()
    if sms_total > 0:
        logger.info(f"SMS: {sms_total:,} records")
        outputs.append({
            "output_type": "SMS",
            "s3_key":      sms_key,
            "records":     sms_total,
            "subdir":      SMS_SUBDIR,
        })
    else:
        logger.warning("SMS: no records found")

    if file_type == "IN":
        for vt in VSS_TYPES:   # orden determinista: 110 → 120 → 130 → 140
            vss_total = vss_writers[vt].close()
            if vss_total > 0:
                logger.info(f"VSS_{vt}: {vss_total:,} records")
                outputs.append({
                    "output_type": f"VSS_{vt}",
                    "s3_key":      vss_keys[vt],
                    "records":     vss_total,
                    "subdir":      VSS_SUBDIRS[vt],
                })
            else:
                logger.warning(f"VSS_{vt}: no records found")
    else:
        logger.info("VSS: skipped (file_type=OUT)")

    return outputs


# =============================================================================
# HANDLER PRINCIPAL
# =============================================================================

def lambda_handler(event, context):
    """
    Entry point del Lambda.

    Input (desde Step Functions):
    {
        "client_id":      "EBGR",
        "file_id":        "ABC123",
        "brand":          "VISA",
        "file_type":      "IN",          # IN o OUT
        "file_date":      "2026-01-03",
        "content_hash":   "XYZ789",
        "s3_key_landing": "EBGR/VS.EBGR.TC00.20260103.001.txt",
        "bucket_landing": "itx-landing-dev"
    }

    Output (para el siguiente paso, Extract):
    {
        "status":        "SUCCESS",
        "total_outputs": 5,
        "total_records": 430000,
        "outputs": [
            {"output_type": "BASEII",   "s3_key": "...", "records": 350000, "subdir": "100-BASEII_RAW_DRAFTS"},
            {"output_type": "SMS",      "s3_key": "...", "records": 80000,  "subdir": "100-SMS_RAW_MESSAGES"},
            {"output_type": "VSS_110",  "s3_key": "...", "records": ...,    "subdir": "100-VSS_110_RAW"},
            {"output_type": "VSS_120",  "s3_key": "...", "records": ...,    "subdir": "100-VSS_120_RAW"},
            {"output_type": "VSS_130",  "s3_key": "...", "records": ...,    "subdir": "100-VSS_130_RAW"},
        ],
        "client_id": ..., "file_id": ..., "brand": ...,
        "file_type": ..., "file_date": ..., "content_hash": ...
    }
    """
    logger.info("=== ITX Transform Lambda ===")
    logger.info(f"Event: {json.dumps(event)}")
    logger.info(f"Config: chunk={CHUNK_SIZE_BYTES // 1024 // 1024}MB, "
                f"flush_batch={FLUSH_BATCH_SIZE:,}")

    if not LANDING_BUCKET or not STAGING_BUCKET:
        raise ValueError(
            "Missing environment variables: S3_BUCKET_LANDING, S3_BUCKET_STAGING"
        )

    client_id    = event['client_id']
    file_id      = event['file_id']
    brand        = event['brand']
    file_type    = event['file_type']
    file_date    = event['file_date']
    content_hash = event['content_hash']
    s3_key       = event['s3_key_landing']
    bucket       = event.get('bucket_landing', LANDING_BUCKET)

    logger.info(f"Processing: client={client_id}, brand={brand}, "
                f"type={file_type}, date={file_date}")

    outputs = _process_single_pass(
        bucket=bucket, key=s3_key,
        client_id=client_id, brand=brand,
        file_type=file_type, file_date=file_date,
        content_hash=content_hash,
    )

    total_records = sum(o['records'] for o in outputs)
    logger.info(f"=== Done: {len(outputs)} outputs, {total_records:,} records ===")

    return {
        'status':        'SUCCESS' if outputs else 'ERROR',
        'total_outputs': len(outputs),
        'total_records': total_records,
        'outputs':       outputs,
        'client_id':     client_id,
        'file_id':       file_id,
        'brand':         brand,
        'file_type':     file_type,
        'file_date':     file_date,
        'content_hash':  content_hash,
    }

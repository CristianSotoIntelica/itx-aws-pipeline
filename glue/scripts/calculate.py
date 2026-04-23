# =============================================================================
# ITX-CALCULATE (PySpark) - AWS Glue Job
# =============================================================================
# Calcula campos adicionales a partir de datos limpios (Clean)
# Soporta: BASEII (drafts), SMS (messages), VSS (settlement 110/120/130/140)
# =============================================================================

import sys
import json
from datetime import datetime, date
import pandas as pd
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, DateType
import boto3


# =============================================================================
# CONFIGURACIÓN SPARK
# =============================================================================

spark = SparkSession.builder \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
logger = glueContext.get_logger()


def log_info(message: str):
    logger.info(f"GlueLogger: {message}")


def log_error(message: str):
    logger.error(f"GlueLogger: {message}")


# =============================================================================
# HELPERS: CARGA DE DATOS DE REFERENCIA
# =============================================================================

def load_reference_table(bucket: str, table_name: str) -> DataFrame:
    """Carga una tabla de referencia desde S3."""
    path = f"s3://{bucket}/{table_name}/data.parquet"
    log_info(f"Loading reference table: {path}")
    return spark.read.parquet(path)


def load_visa_ardef(reference_bucket: str, file_date: date) -> DataFrame:
    """
    Carga y prepara el ARDEF de Visa filtrado para la fecha del archivo.
    Procesado en Pandas en el driver para evitar Window operations sin partición.
    """
    path = f"s3://{reference_bucket}/visa_ardef/data.parquet"
    log_info(f"Loading ARDEF from: {path}")

    # Leer en Pandas — tabla de referencia pequeña, cabe en el driver
    ardef_pd = spark.read.parquet(path).toPandas()

    file_date_str = file_date.strftime("%Y-%m-%d") if isinstance(file_date, date) else str(file_date)
    file_date_obj = pd.to_datetime(file_date_str).date()

    # 1. Filtrar por delete_indicator
    ardef_pd = ardef_pd[ardef_pd["delete_indicator"] == " "]

    # 2. Convertir fechas
    ardef_pd["effective_date"] = pd.to_datetime(
        ardef_pd["effective_date"], errors="coerce"
    ).dt.date
    ardef_pd["valid_until"] = pd.to_datetime(
        ardef_pd["valid_until"], errors="coerce"
    ).dt.date
    ardef_pd["valid_until"] = ardef_pd["valid_until"].fillna(file_date_obj)

    # 3. Filtrar rangos válidos para la fecha
    ardef_pd = ardef_pd[
        (file_date_obj >= ardef_pd["effective_date"]) &
        (file_date_obj <= ardef_pd["valid_until"])
    ]

    # 4. Convertir claves a numérico
    ardef_pd["low_key_for_range"] = pd.to_numeric(
        ardef_pd["low_key_for_range"], errors="coerce"
    )
    ardef_pd["table_key"] = pd.to_numeric(
        ardef_pd["table_key"], errors="coerce"
    )

    # 5. Deduplicar por table_key (effective_date más reciente)
    ardef_pd = ardef_pd.sort_values(
        ["table_key", "effective_date", "low_key_for_range"],
        ascending=[True, False, True]
    )
    ardef_pd = ardef_pd.drop_duplicates(subset="table_key", keep="first")

    # 6. Deduplicar por low_key_for_range
    ardef_pd = ardef_pd.sort_values("low_key_for_range")
    ardef_pd = ardef_pd.drop_duplicates(subset="low_key_for_range", keep="first")

    # 7. Eliminar rangos solapados — SIN Window de Spark, directo en Pandas
    ardef_pd["previous_table_key"] = ardef_pd["table_key"].shift(1)
    ardef_pd = ardef_pd[
        ardef_pd["previous_table_key"].isna() |
        (ardef_pd["low_key_for_range"] > ardef_pd["previous_table_key"])
    ].drop(columns=["previous_table_key"])

    # 8. Seleccionar campos necesarios
    ardef_fields = [
        "low_key_for_range", "table_key", "account_funding_source",
        "ardef_country", 
        #"ardef_region", 
        "b2b_program_id", "country",
        "fast_funds", "nnss_indicator", "product_id", "product_subtype",
        "region", "technology_indicator", "travel_indicator"
    ]
    existing_fields = [f for f in ardef_fields if f in ardef_pd.columns]
    ardef_pd = ardef_pd[existing_fields]

    # 9. Renombrar product_id para evitar conflicto
    if "product_id" in ardef_pd.columns:
        ardef_pd = ardef_pd.rename(columns={"product_id": "ardef_product_id"})

    log_info(f"ARDEF loaded: {len(ardef_pd):,} valid ranges for date {file_date_str}")

    # 10. Convertir a Spark y cachear — se usa múltiples veces en los joins
    ardef_spark = spark.createDataFrame(ardef_pd)
    return ardef_spark.cache(), ardef_pd


def load_country_table(reference_bucket: str) -> DataFrame:
    """Carga la tabla de países."""
    country = load_reference_table(reference_bucket, "country")
    return country.select(
        F.col("country_code"),
        F.col("visa_region_code")
    )


def load_currency_table(reference_bucket: str) -> DataFrame:
    """Carga la tabla de monedas."""
    currency = load_reference_table(reference_bucket, "currency")
    return currency.select(
        F.col("currency_numeric_code"),
        F.col("currency_alphabetic_code")
    )


# =============================================================================
# HELPERS: CARGA DE DATOS DEL CLIENTE (DynamoDB)
# =============================================================================

def get_client_data(client_id: str) -> dict:
    """Obtiene metadatos del cliente desde DynamoDB."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('itx-client')
    
    response = table.get_item(Key={'client_id': client_id})
    
    if 'Item' not in response:
        log_error(f"Client not found: {client_id}")
        return {}
    
    item = response['Item']
    log_info(f"  Client found: {item.get('client_name', client_id)}")
    
    return {
        'client_id': client_id,
        'client_name': item.get('client_name', ''),
        'local_currency_code': item.get('local_currency_code', ''),
        'settlement_currency_code': item.get('settlement_currency_code', ''),
        'report_currency_code': item.get('report_currency_code', ''),
        'issuing_bins_6_digits': item.get('issuing_bins_6_digits', ''),
        'issuing_bins_8_digits': item.get('issuing_bins_8_digits', ''),
        'acquiring_bins': item.get('acquiring_bins', ''),
        'customer_country': item.get('customer_country', ''),
    }


# =============================================================================
# HELPER: JOIN CON ARDEF
# =============================================================================

def join_with_ardef(df: DataFrame, ardef: DataFrame, account_column: str = "account_number") -> DataFrame:
    """
    Range join hiper-optimizado en PySpark usando Bucketing por prefijos.
    Reduce el tiempo de ejecución de minutos a segundos.
    """
    # 1. Crear account_9 (Limpiar asteriscos y tomar los primeros 9 dígitos)
    df = df.withColumn(
        "account_9",
        F.regexp_replace(F.col(account_column), "\\*", "0").substr(1, 9).cast(LongType())
    )
    
    # 🌟 LA MAGIA DE SPARK: CREAR UNA LLAVE EXACTA (PREFIX DE 3 DÍGITOS) 🌟
    # Dividimos entre 1,000,000 para quedarnos con los primeros 3 dígitos (ej. 411)
    df = df.withColumn("join_prefix", F.floor(F.col("account_9") / 1000000).cast(IntegerType()))

    # En ARDEF, identificamos desde qué prefijo hasta qué prefijo va el rango
    ardef_opt = ardef.withColumn("prefix_low", F.floor(F.col("low_key_for_range") / 1000000).cast(IntegerType())) \
                     .withColumn("prefix_high", F.floor(F.col("table_key") / 1000000).cast(IntegerType()))
    
    # Si un rango abarca múltiples prefijos (ej. 411 a 412), creamos una fila para cada uno usando sequence y explode
    ardef_opt = ardef_opt.withColumn("join_prefix", F.explode(F.sequence(F.col("prefix_low"), F.col("prefix_high"))))

    # Limpiamos posibles columnas solapadas del Clean para evitar ambigüedades
    ardef_cols = ardef.columns
    cols_to_drop = [c for c in ardef_cols if c in df.columns and c != "record"]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)

    # 🚀 EL SÚPER JOIN: Primero busca el bucket exacto (O(1)), luego verifica el rango
    df_with_ardef = df.join(
        F.broadcast(ardef_opt),
        on=[
            df["join_prefix"] == ardef_opt["join_prefix"],  # <--- ESTO FUERZA EL BROADCAST HASH JOIN
            df["account_9"] >= ardef_opt["low_key_for_range"],
            df["account_9"] <= ardef_opt["table_key"]
        ],
        how="left"
    )
    
    # Limpieza de columnas temporales
    df_with_ardef = df_with_ardef.drop("low_key_for_range", "table_key", "account_9", "join_prefix", "prefix_low", "prefix_high")
    
    # Deduplicación final (por si ARDEF tenía rangos montados en el mismo bucket)
    window_dedup = Window.partitionBy("record").orderBy(F.col("ardef_country").desc_nulls_last())
    df_with_ardef = df_with_ardef.withColumn("_dedup_rank", F.row_number().over(window_dedup))
    df_with_ardef = df_with_ardef.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")
    
    return df_with_ardef


# =============================================================================
# HELPER: CARGAR Y GUARDAR PARQUET
# =============================================================================

def load_parquet_safe(path: str) -> DataFrame:
    """Carga un archivo Parquet."""
    df = spark.read.parquet(path)
    count = df.count()
    log_info(f"  Loaded {count:,} records from {path}")
    return df


def save_parquet(df: DataFrame, path: str):
    """Guarda DataFrame como Parquet."""
    df.coalesce(1).write.mode("overwrite").parquet(path)
    log_info(f"  Saved to {path}")


# =============================================================================
# CAMPOS CALCULADOS BASEII - GRUPO 1: Campos directos del ARDEF
# =============================================================================

def calc_ardef_country(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_ardef_country", F.col("ardef_country"))


def calc_b2b_program_id(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_b2b_program_id", F.col("b2b_program_id"))


def calc_fast_funds(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_fast_funds", F.col("fast_funds"))


def calc_funding_source(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_funding_source", F.col("account_funding_source"))


def calc_issuer_country(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_issuer_country", F.col("country"))


def calc_nnss_indicator(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_nnss_indicator", F.col("nnss_indicator"))


def calc_product_id_ardef(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_product_id", F.col("ardef_product_id"))


def calc_product_subtype(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_product_subtype", F.col("product_subtype"))


def calc_technology_indicator(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_technology_indicator", F.col("technology_indicator"))


def calc_travel_indicator(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_travel_indicator", F.col("travel_indicator"))


# =============================================================================
# CAMPOS CALCULADOS BASEII - GRUPO 2: String manipulation / coalesce
# =============================================================================

def calc_issuer_bin_8(df: DataFrame, account_column: str = "account_number") -> DataFrame:
    return df.withColumn(
        "calc_issuer_bin_8",
        F.regexp_replace(F.col(account_column), "\\*", "0").substr(1, 8)
    )


def calc_authorization_code_valid_draft(df: DataFrame) -> DataFrame:
    """
    authorization_code_valid para BASEII/draft.
    INVALID si termina en 'x' o si últimos 5 chars están en lista específica.
    """
    invalid_suffixes = [" ", "0000", "00000", "0000n", "0000p", "0000y"]
    
    return df.withColumn(
        "calc_authorization_code_valid",
        F.when(
            F.substring(F.col("authorization_code"), -1, 1) == "x",
            F.lit("INVALID")
        ).when(
            F.substring(F.col("authorization_code"), -5, 5).isin(invalid_suffixes),
            F.lit("INVALID")
        ).otherwise(F.lit("VALID"))
    )


def calc_authorization_code_valid_sms(df: DataFrame) -> DataFrame:
    """authorization_code_valid para SMS usando authorization_id_resp._code"""
    invalid_suffixes = [" ", "0000", "00000", "0000n", "0000p", "0000y"]
    col_name = "authorization_id_resp_code"
    
    return df.withColumn(
        "calc_authorization_code_valid",
        F.when(
            F.substring(F.col(col_name), -1, 1) == "x",
            F.lit("INVALID")
        ).when(
            F.substring(F.col(col_name), -5, 5).isin(invalid_suffixes),
            F.lit("INVALID")
        ).otherwise(F.lit("VALID"))
    )


def calc_business_application_id(df: DataFrame) -> DataFrame:
    """Coalesce de business_application_id_fl, _cr, _ft"""
    return df.withColumn(
        "calc_business_application_id",
        F.coalesce(
            F.when(F.trim(F.col("business_application_id_fl")) != "", F.trim(F.col("business_application_id_fl"))),
            F.when(F.trim(F.col("business_application_id_cr")) != "", F.trim(F.col("business_application_id_cr"))),
            F.when(F.trim(F.col("business_application_id_ft")) != "", F.trim(F.col("business_application_id_ft")))
        )
    )


def calc_business_format_code(df: DataFrame) -> DataFrame:
    """Coalesce de business_format_code_cr, _fl, _ft, _df, _pd, _sd, _sp"""
    return df.withColumn(
        "calc_business_format_code",
        F.coalesce(
            F.when(F.trim(F.col("business_format_code_cr")) != "", F.trim(F.col("business_format_code_cr"))),
            F.when(F.trim(F.col("business_format_code_fl")) != "", F.trim(F.col("business_format_code_fl"))),
            F.when(F.trim(F.col("business_format_code_ft")) != "", F.trim(F.col("business_format_code_ft"))),
            F.when(F.trim(F.col("business_format_code_df")) != "", F.trim(F.col("business_format_code_df"))),
            F.when(F.trim(F.col("business_format_code_pd")) != "", F.trim(F.col("business_format_code_pd"))),
            F.when(F.trim(F.col("business_format_code_sd")) != "", F.trim(F.col("business_format_code_sd"))),
            F.when(F.trim(F.col("business_format_code_sp")) != "", F.trim(F.col("business_format_code_sp")))
        )
    )


def calc_message_reason_code(df: DataFrame) -> DataFrame:
    """Coalesce de message_reason_code_df, _sd, _sp"""
    return df.withColumn(
        "calc_message_reason_code",
        F.coalesce(
            F.when(F.trim(F.col("message_reason_code_df")) != "", F.trim(F.col("message_reason_code_df"))),
            F.when(F.trim(F.col("message_reason_code_sd")) != "", F.trim(F.col("message_reason_code_sd"))),
            F.when(F.trim(F.col("message_reason_code_sp")) != "", F.trim(F.col("message_reason_code_sp")))
        )
    )


def calc_network_identification_code(df: DataFrame) -> DataFrame:
    """Coalesce de network_identification_code_df, _sd, _sp"""
    return df.withColumn(
        "calc_network_identification_code",
        F.coalesce(
            F.when(F.trim(F.col("network_identification_code_df")) != "", F.trim(F.col("network_identification_code_df"))),
            F.when(F.trim(F.col("network_identification_code_sd")) != "", F.trim(F.col("network_identification_code_sd"))),
            F.when(F.trim(F.col("network_identification_code_sp")) != "", F.trim(F.col("network_identification_code_sp")))
        )
    )


def calc_type_of_purchase(df: DataFrame) -> DataFrame:
    """Coalesce de type_of_purchase_fl, _ft"""
    return df.withColumn(
        "calc_type_of_purchase",
        F.coalesce(
            F.when(F.trim(F.col("type_of_purchase_fl")) != "", F.trim(F.col("type_of_purchase_fl"))),
            F.when(F.trim(F.col("type_of_purchase_ft")) != "", F.trim(F.col("type_of_purchase_ft")))
        )
    )


def calc_surcharge_amount(df: DataFrame) -> DataFrame:
    """MAX de surcharge_amount_df, _sd, _sp"""
    return df.withColumn(
        "calc_surcharge_amount",
        F.greatest(
            F.coalesce(F.col("surcharge_amount_df"), F.lit(0)),
            F.coalesce(F.col("surcharge_amount_sd"), F.lit(0)),
            F.coalesce(F.col("surcharge_amount_sp"), F.lit(0))
        )
    )


# =============================================================================
# CAMPOS CALCULADOS BASEII - GRUPO 3: Lógica condicional
# =============================================================================

def calc_business_mode_draft(df: DataFrame, file_type: str) -> DataFrame:
    """
    business_mode para BASEII/draft.
    Basado en draft_code y file_type.
    """
    acquiring_codes_out = ["05", "25", "06", "26", "07", "27"]
    issuing_codes_out = ["15", "35", "16", "36", "17", "37"]
    
    if file_type == "OUT":
        return df.withColumn(
            "calc_business_mode",
            F.when(F.col("draft_code").isin(acquiring_codes_out), F.lit("Acquiring"))
             .when(F.col("draft_code").isin(issuing_codes_out), F.lit("Issuing"))
             .otherwise(F.lit(""))
        )
    else:  # IN
        return df.withColumn(
            "calc_business_mode",
            F.when(F.col("draft_code").isin(acquiring_codes_out), F.lit("Issuing"))
             .when(F.col("draft_code").isin(issuing_codes_out), F.lit("Acquiring"))
             .otherwise(F.lit(""))
        )


def calc_business_mode_sms(df: DataFrame) -> DataFrame:
    """business_mode para SMS basado en issuer_acquirer_indicator."""
    return df.withColumn(
        "calc_business_mode",
        F.when(F.col("issuer_acquirer_indicator") == "A", F.lit("Acquiring"))
         .when(F.col("issuer_acquirer_indicator") == "I", F.lit("Issuing"))
         .otherwise(F.lit(""))
    )


def calc_business_transaction_type_draft(df: DataFrame) -> DataFrame:
    """
    business_transaction_type para BASEII/draft.
    Lógica compleja basada en draft_code, MCC, usage_code, etc.
    """
    purchase_codes = ["05", "15", "25", "35"]
    cash_codes = ["06", "16", "26", "36"]
    atm_codes = ["07", "17", "27", "37"]
    special_mcc = [4829, 6051, 7995]
    
    return df.withColumn(
        "calc_business_transaction_type",
        F.when(
            F.col("draft_code").isin(purchase_codes) & ~F.col("merchant_category_code").isin(special_mcc),
            F.lit(1)
        ).when(
            F.col("draft_code").isin(purchase_codes) & F.col("merchant_category_code").isin(special_mcc),
            F.lit(3)
        ).when(
            F.col("draft_code").isin(cash_codes) & (F.col("usage_code") == 1) &
            F.col("special_condition_indicator_merchant_draft_indicator").isin(["7", "8"]),
            F.lit(20)
        ).when(
            F.col("draft_code").isin(cash_codes) & (F.col("usage_code") == 1) &
            (F.col("draft_code_qualifier_0") == 2),
            F.lit(25)
        ).when(
            F.col("draft_code").isin(cash_codes) & (F.col("usage_code") == 1),
            F.lit(19)
        ).when(
            F.col("draft_code").isin(atm_codes) & (F.col("merchant_category_code") == 6010),
            F.lit(21)
        ).when(
            F.col("draft_code").isin(atm_codes) & (F.col("merchant_category_code") == 6011),
            F.lit(22)
        ).otherwise(F.lit(255))
    )


def calc_business_transaction_type_sms(df: DataFrame) -> DataFrame:
    """business_transaction_type para SMS."""
    df = df.withColumn("_rmt", F.col("request_message_type"))
    df = df.withColumn("_rc", F.col("response_code"))
    df = df.withColumn("_pc", F.substring(F.col("processing_code"), 1, 2))
    df = df.withColumn("_pos", F.col("pos_condition_code"))
    df = df.withColumn("_mcc", F.col("`merchant's_type`"))
    
    cond_success = F.col("_rmt").isin(["0200", "0220", "0400", "0420"]) & (F.col("_rc") == "00")
    cond_decline = F.col("_rmt").isin(["0200", "0220", "0400", "0420"]) & (F.col("_rc") != "00")
    not_special_pos = ~F.col("_pos").isin(["13", "51"])
    not_special_mcc = ~F.col("_mcc").isin([4815, 6010, 6011])
    
    df = df.withColumn(
        "calc_business_transaction_type",
        F.when(cond_success & (F.col("_pc") == "00") & not_special_pos & not_special_mcc, F.lit(1))
         .when(cond_success & (F.col("_pc") == "01") & not_special_pos & (F.col("_mcc") == 6010), F.lit(21))
         .when(cond_success & (F.col("_pc") == "01") & not_special_pos & (F.col("_mcc") == 6011), F.lit(22))
         .when(cond_success & (F.col("_pc") == "10") & not_special_pos & not_special_mcc, F.lit(30))
         .when(cond_success & (F.col("_pc") == "11") & not_special_pos & not_special_mcc, F.lit(3))
         .when(cond_success & (F.col("_pc") == "19") & not_special_pos & not_special_mcc, F.lit(115))
         .when(cond_success & (F.col("_pc") == "20") & not_special_pos & not_special_mcc, F.lit(19))
         .when(cond_success & (F.col("_pc") == "22") & F.col("_pos").isin(["13"]) & not_special_mcc, F.lit(20))
         .when(cond_success & (F.col("_pc") == "26") & not_special_pos & not_special_mcc, F.lit(25))
         .when(cond_success & (F.col("_pc") == "29") & not_special_pos & not_special_mcc, F.lit(200))
         .when(cond_success & (F.col("_pc") == "30") & not_special_pos & (F.col("_mcc") == 6011), F.lit(247))
         .when(cond_success & (F.col("_pc") == "40") & not_special_pos & (F.col("_mcc") == 6011), F.lit(250))
         .when(cond_success & (F.col("_pc") == "50") & not_special_pos & not_special_mcc, F.lit(27))
         .when(cond_decline & (F.col("_mcc") != 6011), F.lit(236))
         .when(cond_decline & (F.col("_mcc") == 6011), F.lit(249))
         .otherwise(F.lit(None).cast(IntegerType()))
    )
    
    df = df.drop("_rmt", "_rc", "_pc", "_pos", "_mcc")
    return df


def calc_reversal_indicator_draft(df: DataFrame) -> DataFrame:
    """reversal_indicator para BASEII/draft."""
    reversal_codes = ["25", "26", "27", "35", "36", "37"]
    return df.withColumn(
        "calc_reversal_indicator",
        F.when(F.col("draft_code").isin(reversal_codes), F.lit(1)).otherwise(F.lit(0))
    )


def calc_reversal_indicator_sms(df: DataFrame) -> DataFrame:
    """reversal_indicator para SMS."""
    return df.withColumn(
        "calc_reversal_indicator",
        F.when(
            F.col("request_message_type").isin(["0200", "0220"]) & (F.col("response_code") == "00"),
            F.lit(0)
        ).when(
            F.col("request_message_type").isin(["0400", "0420"]) & (F.col("response_code") == "00"),
            F.lit(1)
        ).otherwise(F.lit(0))
    )


def calc_jurisdiction_country_draft(df: DataFrame) -> DataFrame:
    """jurisdiction_country para BASEII/draft = merchant_country_code"""
    return df.withColumn("calc_jurisdiction_country", F.col("merchant_country_code"))


def calc_jurisdiction_country_sms(df: DataFrame) -> DataFrame:
    """jurisdiction_country para SMS = card_acceptor_country"""
    return df.withColumn("calc_jurisdiction_country", F.col("card_acceptor_country"))


# =============================================================================
# CAMPOS CALCULADOS BASEII - GRUPO 4: JOINs con country/currency
# =============================================================================

def calc_issuer_region(df: DataFrame, country_df: DataFrame) -> DataFrame:
    """issuer_region: JOIN country del ARDEF con tabla country."""
    country_for_issuer = country_df.select(
        F.col("country_code").alias("_country_code_issuer"),
        F.col("visa_region_code").alias("calc_issuer_region")
    )
    
    df = df.join(
        F.broadcast(country_for_issuer),
        F.col("country") == F.col("_country_code_issuer"),
        how="left"
    ).drop("_country_code_issuer")
    
    return df


def calc_jurisdiction_region_draft(df: DataFrame, country_df: DataFrame) -> DataFrame:
    """jurisdiction_region para BASEII/draft: JOIN merchant_country_code."""
    country_for_merchant = country_df.select(
        F.col("country_code").alias("_country_code_merchant"),
        F.col("visa_region_code").alias("calc_jurisdiction_region")
    )
    
    df = df.join(
        F.broadcast(country_for_merchant),
        F.col("merchant_country_code") == F.col("_country_code_merchant"),
        how="left"
    ).drop("_country_code_merchant")
    
    return df


def calc_jurisdiction_region_sms(df: DataFrame, country_df: DataFrame) -> DataFrame:
    """jurisdiction_region para SMS: JOIN card_acceptor_country."""
    country_for_merchant = country_df.select(
        F.col("country_code").alias("_country_code_merchant"),
        F.col("visa_region_code").alias("calc_jurisdiction_region")
    )
    
    df = df.join(
        F.broadcast(country_for_merchant),
        F.col("card_acceptor_country") == F.col("_country_code_merchant"),
        how="left"
    ).drop("_country_code_merchant")
    
    return df


def calc_source_currency_code_alphabetic_draft(df: DataFrame, currency_df: DataFrame) -> DataFrame:
    """source_currency_code_alphabetic para BASEII/draft."""
    currency_lookup = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric"),
        F.col("currency_alphabetic_code").alias("calc_source_currency_code_alphabetic")
    )
    
    df = df.join(
        F.broadcast(currency_lookup),
        F.col("source_currency_code") == F.col("_currency_numeric"),
        how="left"
    ).drop("_currency_numeric")
    
    return df


def calc_source_currency_code_alphabetic_sms(df: DataFrame, currency_df: DataFrame) -> DataFrame:
    """source_currency_code_alphabetic para SMS usando draft_currency_code."""
    currency_lookup = currency_df.select(
        F.col("currency_numeric_code").alias("_currency_numeric"),
        F.col("currency_alphabetic_code").alias("calc_source_currency_code_alphabetic")
    )
    
    df = df.join(
        F.broadcast(currency_lookup),
        F.col("draft_currency_code") == F.col("_currency_numeric"),
        how="left"
    ).drop("_currency_numeric")
    
    return df


# =============================================================================
# JURISDICTION - DRAFT
# =============================================================================

def calc_jurisdiction_draft(df: DataFrame, country_df: DataFrame, file_type: str, client_data: dict) -> DataFrame:
    """
    jurisdiction para BASEII/draft.
    Replica exacta de la lógica original en Pandas.
    """
    issuing_bins_6 = [b.strip() for b in str(client_data.get('issuing_bins_6_digits', '')).split(',') if b.strip()]
    issuing_bins_8 = [b.strip() for b in str(client_data.get('issuing_bins_8_digits', '')).split(',') if b.strip()]
    acquiring_bins = [b.strip() for b in str(client_data.get('acquiring_bins', '')).split(',') if b.strip()]
    
    # JOIN para obtener merchant_region_code
    country_merchant = country_df.select(
        F.col("country_code").alias("_mc_country"),
        F.col("visa_region_code").alias("merchant_region_code")
    )
    df = df.join(
        F.broadcast(country_merchant),
        F.col("merchant_country_code") == F.col("_mc_country"),
        how="left"
    ).drop("_mc_country")
    
    # JOIN para obtener ardef_region
    country_ardef = country_df.select(
        F.col("country_code").alias("_ac_country"),
        F.col("visa_region_code").alias("ardef_region")
    )
    df = df.join(
        F.broadcast(country_ardef),
        F.col("ardef_country") == F.col("_ac_country"),
        how="left"
    ).drop("_ac_country")
    
    # Columnas auxiliares para BIN matching
    df = df.withColumn(
        "_account_6",
        F.regexp_replace(F.col("account_number"), "\\*", "0").substr(1, 6)
    ).withColumn(
        "_account_8",
        F.regexp_replace(F.col("account_number"), "\\*", "0").substr(1, 8)
    ).withColumn(
        "_acq_id_padded",
        F.lpad(F.col("account_reference_number_acquiring_identifier").cast(StringType()), 6, "0")
    )
    
    # Condiciones base
    same_country = F.col("merchant_country_code") == F.col("ardef_country")
    collection_flag = F.col("collection_only_flag") == "C"
    
    # Matching de BINs
    issuing_6_match = F.col("_account_6").isin(issuing_bins_6) if issuing_bins_6 else F.lit(False)
    issuing_8_match = F.col("_account_8").isin(issuing_bins_8) if issuing_bins_8 else F.lit(False)
    acquiring_match = F.col("_acq_id_padded").isin(acquiring_bins) if acquiring_bins else F.lit(False)
    
    # Lógica según file_type (exactamente como el original)
    if file_type == "OUT":
        on_us_condition = same_country & (collection_flag | issuing_6_match | issuing_8_match)
    else:  # IN
        on_us_condition = same_country & (collection_flag | acquiring_match)
    
    # Aplicar condiciones en orden
    df = df.withColumn(
        "calc_jurisdiction",
        F.when(on_us_condition, F.lit("on-us"))
         .when(same_country, F.lit("off-us"))
         .when((~same_country) & (F.col("merchant_region_code") == F.col("ardef_region")), F.lit("intraregional"))
         .when((~same_country) & (F.col("merchant_region_code") != F.col("ardef_region")), F.lit("interregional"))
         .otherwise(F.lit(""))
    )
    
    df = df.drop("_account_6", "_account_8", "_acq_id_padded")
    
    return df


def calc_jurisdiction_assigned_draft(df: DataFrame) -> DataFrame:
    """
    jurisdiction_assigned para BASEII/draft.
    Lógica exacta del original.
    """
    return df.withColumn(
        "calc_jurisdiction_assigned",
        F.when(
            F.col("merchant_country_code") == F.col("ardef_country"),
            F.col("merchant_country_code")
        ).when(
            (F.col("merchant_country_code") != F.col("ardef_country")) &
            (F.col("merchant_region_code") == F.col("ardef_region")),
            F.col("ardef_region")
        ).when(
            (F.col("merchant_country_code") != F.col("ardef_country")) &
            (F.col("merchant_region_code") != F.col("ardef_region")),
            F.lit("9")
        ).otherwise(F.lit(""))
    )


# =============================================================================
# JURISDICTION - SMS
# =============================================================================

def calc_jurisdiction_sms(df: DataFrame, country_df: DataFrame, client_data: dict) -> DataFrame:
    """
    jurisdiction para SMS.
    NOTA: El original hace merge left_on='card_acceptor_country', right_on='merchant_country_code'
    Esto AGREGA merchant_country_code como columna al DataFrame.
    """
    issuing_bins_6 = [b.strip() for b in str(client_data.get('issuing_bins_6_digits', '')).split(',') if b.strip()]
    issuing_bins_8 = [b.strip() for b in str(client_data.get('issuing_bins_8_digits', '')).split(',') if b.strip()]
    acquiring_bins = [b.strip() for b in str(client_data.get('acquiring_bins', '')).split(',') if b.strip()]
    
    # JOIN para obtener merchant_country_code y merchant_region_code
    # Simula: pd.merge(source, country, left_on="card_acceptor_country", right_on="merchant_country_code")
    country_merchant = country_df.select(
        F.col("country_code").alias("merchant_country_code"),
        F.col("visa_region_code").alias("merchant_region_code")
    )
    df = df.join(
        F.broadcast(country_merchant),
        F.col("card_acceptor_country") == F.col("merchant_country_code"),
        how="left"
    )
    
    # JOIN para obtener ardef_region
    country_ardef = country_df.select(
        F.col("country_code").alias("_ac_country"),
        F.col("visa_region_code").alias("ardef_region")
    )
    df = df.join(
        F.broadcast(country_ardef),
        F.col("ardef_country") == F.col("_ac_country"),
        how="left"
    ).drop("_ac_country")
    
    # Columnas auxiliares
    df = df.withColumn(
        "_card_6",
        F.regexp_replace(F.col("card_number"), "\\*", "0").substr(1, 6)
    ).withColumn(
        "_card_8",
        F.regexp_replace(F.col("card_number"), "\\*", "0").substr(1, 8)
    ).withColumn(
        "_acq_id_padded",
        F.lpad(F.col("acquiring_institution_id_1").cast(StringType()), 6, "0")
    )
    
    # Condiciones
    same_country = F.col("merchant_country_code") == F.col("ardef_country")
    
    issuing_6_match = F.col("_card_6").isin(issuing_bins_6) if issuing_bins_6 else F.lit(False)
    issuing_8_match = F.col("_card_8").isin(issuing_bins_8) if issuing_bins_8 else F.lit(False)
    acquiring_match = F.col("_acq_id_padded").isin(acquiring_bins) if acquiring_bins else F.lit(False)
    
    # on-us para Acquiring (indicator=A): usa issuing BINs
    # on-us para Issuing (indicator=I): usa acquiring BINs
    on_us_acquiring = same_country & (F.col("issuer_acquirer_indicator") == "A") & (issuing_6_match | issuing_8_match)
    on_us_issuing = same_country & (F.col("issuer_acquirer_indicator") == "I") & acquiring_match
    
    df = df.withColumn(
        "calc_jurisdiction",
        F.when(on_us_acquiring | on_us_issuing, F.lit("on-us"))
         .when(same_country, F.lit("off-us"))
         .when((~same_country) & (F.col("merchant_region_code") == F.col("ardef_region")), F.lit("intraregional"))
         .when((~same_country) & (F.col("merchant_region_code") != F.col("ardef_region")), F.lit("interregional"))
         .otherwise(F.lit(""))
    )
    
    df = df.drop("_card_6", "_card_8", "_acq_id_padded")
    
    return df


def calc_jurisdiction_assigned_sms(df: DataFrame) -> DataFrame:
    """
    jurisdiction_assigned para SMS.
    Usa merchant_country_code (agregado por el JOIN en calc_jurisdiction_sms).
    """
    return df.withColumn(
        "calc_jurisdiction_assigned",
        F.when(
            F.col("merchant_country_code") == F.col("ardef_country"),
            F.col("merchant_country_code")
        ).when(
            (F.col("merchant_country_code") != F.col("ardef_country")) &
            (F.col("merchant_region_code") == F.col("ardef_region")),
            F.col("ardef_region")
        ).when(
            (F.col("merchant_country_code") != F.col("ardef_country")) &
            (F.col("merchant_region_code") != F.col("ardef_region")),
            F.lit("9")
        ).otherwise(F.lit(""))
    )


# =============================================================================
# TIMELINESS
# =============================================================================

def calc_timeliness_draft(df: DataFrame) -> DataFrame:
    """
    timeliness para BASEII/draft.
    Días entre purchase_date y central_processing_date, excluyendo domingos.
    """
    df = df.withColumn("_central_date", F.to_date(F.col("central_processing_date")))
    df = df.withColumn("_purchase_date", F.to_date(F.col("purchase_date")))
    df = df.withColumn("_total_days", F.datediff(F.col("_central_date"), F.col("_purchase_date")))
    
    df = df.withColumn("_start_for_sundays", F.date_add(F.col("_purchase_date"), 1))
    df = df.withColumn("_end_for_sundays", F.date_sub(F.col("_central_date"), 1))
    df = df.withColumn("_days_between", F.datediff(F.col("_end_for_sundays"), F.col("_start_for_sundays")) + 1)
    df = df.withColumn("_start_dow", F.dayofweek(F.col("_start_for_sundays")))
    df = df.withColumn("_full_weeks", F.floor(F.col("_days_between") / 7))
    df = df.withColumn("_remaining_days", F.col("_days_between") % 7)
    df = df.withColumn("_days_to_next_sunday", F.when(F.col("_start_dow") == 1, F.lit(0)).otherwise(8 - F.col("_start_dow")))
    df = df.withColumn("_extra_sunday", F.when((F.col("_days_between") > 0) & (F.col("_remaining_days") >= F.col("_days_to_next_sunday")), F.lit(1)).otherwise(F.lit(0)))
    df = df.withColumn("_sundays_count", F.when(F.col("_days_between") <= 0, F.lit(0)).otherwise(F.col("_full_weeks") + F.col("_extra_sunday")))
    
    df = df.withColumn(
        "calc_timeliness",
        F.when(F.col("_total_days") == 0, F.lit(0))
         .when(F.col("_total_days").isNull() | F.col("_purchase_date").isNull() | F.col("_central_date").isNull(), F.lit(None))
         .otherwise(F.col("_total_days") - 1 - F.col("_sundays_count"))
    )
    
    df = df.drop("_central_date", "_purchase_date", "_total_days", "_start_for_sundays", "_end_for_sundays",
                 "_days_between", "_start_dow", "_full_weeks", "_remaining_days", "_days_to_next_sunday",
                 "_extra_sunday", "_sundays_count")
    
    return df


def calc_timeliness_sms(df: DataFrame) -> DataFrame:
    """timeliness para SMS: settlement_date_sms - local_draft_date"""
    return df.withColumn(
        "calc_timeliness",
        F.datediff(F.to_date(F.col("settlement_date_sms")), F.to_date(F.col("local_draft_date")))
    )


# =============================================================================
# CAMPOS ESPECÍFICOS SMS
# =============================================================================

def calc_acquirer_bin(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_acquirer_bin", F.substring(F.col("retrieval_reference_number"), 1, 6))


def calc_processing_code_transaction_type(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_processing_code_transaction_type", F.substring(F.col("processing_code"), 1, 2))


def calc_source_amount_sms(df: DataFrame) -> DataFrame:
    return df.withColumn("calc_source_amount", F.col("draft_amount"))


def calc_transaction_code_sms(df: DataFrame) -> DataFrame:
    """transaction_code_sms basado en business_transaction_type + reversal_indicator."""
    df = df.withColumn(
        "_transaction_type",
        F.when(F.col("calc_business_transaction_type").isin([1, 30, 27]), F.lit("PUR"))
         .when(F.col("calc_business_transaction_type").isin([3, 115]), F.lit("CRD"))
         .when(F.col("calc_business_transaction_type").isin([19, 20, 21, 22, 25]), F.lit("CSH"))
         .otherwise(F.lit(None))
    )
    
    df = df.withColumn(
        "calc_transaction_code_sms",
        F.when((F.col("_transaction_type") == "PUR") & (F.col("calc_reversal_indicator") == 0), F.lit("05"))
         .when((F.col("_transaction_type") == "CRD") & (F.col("calc_reversal_indicator") == 0), F.lit("06"))
         .when((F.col("_transaction_type") == "CSH") & (F.col("calc_reversal_indicator") == 0), F.lit("07"))
         .when((F.col("_transaction_type") == "PUR") & (F.col("calc_reversal_indicator") == 1), F.lit("25"))
         .when((F.col("_transaction_type") == "CRD") & (F.col("calc_reversal_indicator") == 1), F.lit("26"))
         .when((F.col("_transaction_type") == "CSH") & (F.col("calc_reversal_indicator") == 1), F.lit("27"))
         .otherwise(F.lit(""))
    )
    
    df = df.drop("_transaction_type")
    return df


# =============================================================================
# CAMPOS CALCULADOS VSS
# =============================================================================

def calc_vss_report_type(df: DataFrame, vss_type: str) -> DataFrame:
    """vss_report_type: int del vss_type (110, 120, 130, 140)"""
    return df.withColumn("calc_vss_report_type", F.lit(int(vss_type)).cast(LongType()))


def calc_vss_aggregation_level(df: DataFrame, vss_type: str) -> DataFrame:
    """
    vss_aggregation_level con lógica recursiva completa.
    - Level 10: Top level (rollup_to == reporting_for)
    - Level 1-3: Intermediate levels
    - Level 0: Base level (default)
    """
    rollup_col = f"rollup_to_sre_identifier_{vss_type}"
    reporting_col = f"reporting_for_sre_identifier_{vss_type}"
    
    if rollup_col not in df.columns or reporting_col not in df.columns:
        log_info(f"  Warning: Missing rollup columns for VSS {vss_type}. Setting aggregation_level to 0.")
        return df.withColumn("calc_vss_aggregation_level", F.lit(0).cast(LongType()))
    
    # Añadir row_id único
    df = df.withColumn("_row_id", F.monotonically_increasing_id())
    
    # Condición top level
    top_level_condition = F.col(rollup_col) == F.col(reporting_col)
    
    # Identificar rollup groups (nodos intermedios)
    rollup_groups_df = df.filter(
        F.col(rollup_col) != F.col(reporting_col)
    ).select(
        F.col(rollup_col).alias("_rollup_group_id")
    ).distinct()
    
    rollup_groups_df = F.broadcast(rollup_groups_df)
    
    # Lookup para subir en jerarquía
    hierarchy_lookup = df.select(
        F.col(reporting_col).alias("_lookup_reporting"),
        F.col(rollup_col).alias("_lookup_rollup")
    ).distinct()
    
    # Inicializar
    df = df.withColumn(
        "calc_vss_aggregation_level",
        F.when(top_level_condition, F.lit(10)).otherwise(F.lit(0))
    )
    df = df.withColumn("_current_reporting", F.col(reporting_col))
    
    # Calcular niveles 1, 2, 3
    for level in [1, 2, 3]:
        df = df.join(
            rollup_groups_df.withColumn("_in_rollup_group", F.lit(True)),
            F.col("_current_reporting") == F.col("_rollup_group_id"),
            how="left"
        ).drop("_rollup_group_id")
        
        df = df.withColumn(
            "calc_vss_aggregation_level",
            F.when(
                (F.col("calc_vss_aggregation_level") == 0) &
                (~top_level_condition) &
                (F.col("_in_rollup_group") == True),
                F.lit(level)
            ).otherwise(F.col("calc_vss_aggregation_level"))
        )
        
        df = df.join(
            F.broadcast(hierarchy_lookup),
            F.col("_current_reporting") == F.col("_lookup_reporting"),
            how="left"
        ).withColumn(
            "_current_reporting",
            F.coalesce(F.col("_lookup_rollup"), F.lit(""))
        ).drop("_lookup_reporting", "_lookup_rollup", "_in_rollup_group")
    
    df = df.drop("_row_id", "_current_reporting")
    df = df.withColumn("calc_vss_aggregation_level", F.col("calc_vss_aggregation_level").cast(LongType()))
    
    return df


# =============================================================================
# FUNCIONES PRINCIPALES DE CÁLCULO
# =============================================================================

def calculate_baseii_fields(df: DataFrame, ardef: DataFrame, ardef_pd: DataFrame, country_df: DataFrame,
                            currency_df: DataFrame, file_type: str, client_data: dict) -> DataFrame:
    """Calcula todos los campos adicionales para BASEII (28 campos)."""
    log_info("Calculating BASEII fields")
    log_info(f"Input records: {df.count():,}")
    
    # 1. JOIN con ARDEF
    log_info("  Joining with ARDEF...")
    df = join_with_ardef(df, ardef, "account_number")
    
    # 2. Campos directos del ARDEF
    log_info("  Calculating ARDEF-based fields...")
    df = calc_ardef_country(df)
    df = calc_b2b_program_id(df)
    df = calc_fast_funds(df)
    df = calc_funding_source(df)
    df = calc_issuer_country(df)
    df = calc_nnss_indicator(df)
    df = calc_product_id_ardef(df)
    df = calc_product_subtype(df)
    df = calc_technology_indicator(df)
    df = calc_travel_indicator(df)
    
    # 3. String manipulation
    log_info("  Calculating string/coalesce fields...")
    df = calc_issuer_bin_8(df, "account_number")
    df = calc_authorization_code_valid_draft(df)
    df = calc_business_application_id(df)
    df = calc_business_format_code(df)
    df = calc_message_reason_code(df)
    df = calc_network_identification_code(df)
    df = calc_type_of_purchase(df)
    df = calc_surcharge_amount(df)
    
    # 4. Lógica condicional
    log_info("  Calculating conditional fields...")
    df = calc_business_mode_draft(df, file_type)
    df = calc_business_transaction_type_draft(df)
    df = calc_reversal_indicator_draft(df)
    df = calc_jurisdiction_country_draft(df)
    
    # 5. JOINs adicionales
    log_info("  Calculating fields with reference table JOINs...")
    df = calc_issuer_region(df, country_df)
    df = calc_jurisdiction_region_draft(df, country_df)
    df = calc_source_currency_code_alphabetic_draft(df, currency_df)
    
    # 6. Jurisdiction (agrega merchant_region_code y ardef_region)
    log_info("  Calculating jurisdiction fields...")
    df = calc_jurisdiction_draft(df, country_df, file_type, client_data)
    df = calc_jurisdiction_assigned_draft(df)
    
    # 7. Timeliness
    log_info("  Calculating timeliness...")
    df = calc_timeliness_draft(df)
    
    # 8. Seleccionar output
    log_info("  Selecting final output columns...")
    output_columns = [
        "record",
        F.col("calc_ardef_country").alias("ardef_country"),
        F.col("calc_authorization_code_valid").alias("authorization_code_valid"),
        F.col("calc_b2b_program_id").alias("b2b_program_id"),
        F.col("calc_business_application_id").alias("business_application_id"),
        F.col("calc_business_format_code").alias("business_format_code"),
        F.col("calc_business_mode").alias("business_mode"),
        F.col("calc_business_transaction_type").alias("business_transaction_type"),
        F.col("calc_fast_funds").alias("fast_funds"),
        F.col("calc_funding_source").alias("funding_source"),
        F.col("calc_issuer_bin_8").alias("issuer_bin_8"),
        F.col("calc_issuer_country").alias("issuer_country"),
        F.col("calc_issuer_region").alias("issuer_region"),
        F.col("calc_jurisdiction").alias("jurisdiction"),
        F.col("calc_jurisdiction_assigned").alias("jurisdiction_assigned"),
        F.col("calc_jurisdiction_country").alias("jurisdiction_country"),
        F.col("calc_jurisdiction_region").alias("jurisdiction_region"),
        F.col("calc_message_reason_code").alias("message_reason_code"),
        F.col("calc_network_identification_code").alias("network_identification_code"),
        F.col("calc_nnss_indicator").alias("nnss_indicator"),
        F.col("calc_product_id").alias("product_id"),
        F.col("calc_product_subtype").alias("product_subtype"),
        F.col("calc_reversal_indicator").alias("reversal_indicator"),
        F.col("calc_source_currency_code_alphabetic").alias("source_currency_code_alphabetic"),
        F.col("calc_surcharge_amount").alias("surcharge_amount"),
        F.col("calc_technology_indicator").alias("technology_indicator"),
        F.col("calc_timeliness").alias("timeliness"),
        F.col("calc_travel_indicator").alias("travel_indicator"),
        F.col("calc_type_of_purchase").alias("type_of_purchase"),
    ]
    
    result = df.select(output_columns)
    log_info(f"BASEII calculation complete. Output columns: {len(output_columns)}")
    return result


def calculate_sms_fields(df: DataFrame, ardef: DataFrame, ardef_pd: DataFrame, country_df: DataFrame,
                         currency_df: DataFrame, client_data: dict) -> DataFrame:
    """Calcula todos los campos adicionales para SMS (26 campos)."""
    log_info("Calculating SMS fields")
    log_info(f"Input records: {df.count():,}")
    
    # 1. JOIN con ARDEF usando card_number
    log_info("  Joining with ARDEF...")
    df = join_with_ardef(df, ardef, "card_number")
    
    # 2. Campos directos del ARDEF
    log_info("  Calculating ARDEF-based fields...")
    df = calc_ardef_country(df)
    df = calc_b2b_program_id(df)
    df = calc_fast_funds(df)
    df = calc_funding_source(df)
    df = calc_issuer_country(df)
    df = calc_nnss_indicator(df)
    df = calc_product_id_ardef(df)
    df = calc_product_subtype(df)
    df = calc_technology_indicator(df)
    df = calc_travel_indicator(df)
    
    # 3. Campos específicos SMS
    log_info("  Calculating SMS-specific fields...")
    df = calc_acquirer_bin(df)
    df = calc_issuer_bin_8(df, "card_number")
    df = calc_authorization_code_valid_sms(df)
    df = calc_processing_code_transaction_type(df)
    df = calc_source_amount_sms(df)
    
    # 4. Lógica condicional
    log_info("  Calculating conditional fields...")
    df = calc_business_mode_sms(df)
    df = calc_business_transaction_type_sms(df)
    df = calc_reversal_indicator_sms(df)
    df = calc_jurisdiction_country_sms(df)
    
    # 5. JOINs adicionales
    log_info("  Calculating fields with reference table JOINs...")
    df = calc_issuer_region(df, country_df)
    df = calc_jurisdiction_region_sms(df, country_df)
    df = calc_source_currency_code_alphabetic_sms(df, currency_df)
    
    # 6. Jurisdiction (agrega merchant_country_code, merchant_region_code, ardef_region)
    log_info("  Calculating jurisdiction fields...")
    df = calc_jurisdiction_sms(df, country_df, client_data)
    df = calc_jurisdiction_assigned_sms(df)
    
    # 7. Timeliness y transaction_code
    log_info("  Calculating timeliness and transaction_code...")
    df = calc_timeliness_sms(df)
    df = calc_transaction_code_sms(df)
    
    # 8. Seleccionar output
    log_info("  Selecting final output columns...")
    output_columns = [
        "record",
        F.col("calc_acquirer_bin").alias("acquirer_bin"),
        F.col("calc_ardef_country").alias("ardef_country"),
        F.col("calc_authorization_code_valid").alias("authorization_code_valid"),
        F.col("calc_b2b_program_id").alias("b2b_program_id"),
        F.col("calc_business_mode").alias("business_mode"),
        F.col("calc_business_transaction_type").alias("business_transaction_type"),
        F.col("calc_fast_funds").alias("fast_funds"),
        F.col("calc_funding_source").alias("funding_source"),
        F.col("calc_issuer_bin_8").alias("issuer_bin_8"),
        F.col("calc_issuer_country").alias("issuer_country"),
        F.col("calc_issuer_region").alias("issuer_region"),
        F.col("calc_jurisdiction").alias("jurisdiction"),
        F.col("calc_jurisdiction_assigned").alias("jurisdiction_assigned"),
        F.col("calc_jurisdiction_country").alias("jurisdiction_country"),
        F.col("calc_jurisdiction_region").alias("jurisdiction_region"),
        F.col("calc_nnss_indicator").alias("nnss_indicator"),
        F.col("calc_processing_code_transaction_type").alias("processing_code_transaction_type"),
        F.col("calc_product_id").alias("product_id"),
        F.col("calc_product_subtype").alias("product_subtype"),
        F.col("calc_reversal_indicator").alias("reversal_indicator"),
        F.col("calc_source_amount").alias("source_amount"),
        F.col("calc_source_currency_code_alphabetic").alias("source_currency_code_alphabetic"),
        F.col("calc_technology_indicator").alias("technology_indicator"),
        F.col("calc_timeliness").alias("timeliness"),
        F.col("calc_transaction_code_sms").alias("transaction_code_sms"),
        F.col("calc_travel_indicator").alias("travel_indicator"),
    ]
    
    result = df.select(output_columns)
    log_info(f"SMS calculation complete. Output columns: {len(output_columns)}")
    return result


def calculate_vss_fields(df: DataFrame, vss_type: str) -> DataFrame:
    """Calcula los campos para VSS (2 campos)."""
    log_info(f"Calculating VSS {vss_type} fields")
    log_info(f"Input records: {df.count():,}")
    
    df = calc_vss_report_type(df, vss_type)
    df = calc_vss_aggregation_level(df, vss_type)
    
    output_columns = [
        "record",
        F.col("calc_vss_report_type").alias("vss_report_type"),
        F.col("calc_vss_aggregation_level").alias("vss_aggregation_level"),
    ]
    
    result = df.select(output_columns)
    log_info(f"VSS {vss_type} calculation complete. Output columns: {len(output_columns)}")
    return result


# =============================================================================
# HELPER: PROCESAR OUTPUT
# =============================================================================

def process_output(output_config: dict, staging_bucket: str, reference_bucket: str,
                   file_type: str, file_date: str, client_data: dict, ardef: DataFrame, ardef_pd,
                   country_df: DataFrame, currency_df: DataFrame) -> dict:
    """
    Procesa un output específico (BASEII, SMS, o VSS_xxx).
    SIN try/except - errores suben y matan el job (Step Functions verá rojo).
    """
    output_type = output_config.get('output_type', '')
    input_s3_key = output_config.get('s3_key', '')
    
    if not input_s3_key:
        raise ValueError(f"No s3_key in output_config for {output_type}")
    
    input_path = f"s3://{staging_bucket}/{input_s3_key}"
    output_s3_key = input_s3_key.replace('/300_', '/400_').replace('_cln', '_cal')
    output_path = f"s3://{staging_bucket}/{output_s3_key}"
    
    log_info(f"Processing {output_type}")
    log_info(f"  Input:  {input_path}")
    log_info(f"  Output: {output_path}")
    
    df = load_parquet_safe(input_path)
    
    if output_type == 'BASEII':
        result_df = calculate_baseii_fields(df, ardef, ardef_pd, country_df, currency_df, file_type, client_data)
    elif output_type == 'SMS':
        result_df = calculate_sms_fields(df, ardef, ardef_pd, country_df, currency_df, client_data)
    elif output_type.startswith('VSS_'):
        vss_type = output_type.replace('VSS_', '')
        result_df = calculate_vss_fields(df, vss_type)
    else:
        raise ValueError(f"Unknown output_type: {output_type}")
    
    result_df = result_df.cache()
    record_count = result_df.count()
    save_parquet(result_df, output_path)
    result_df.unpersist()
    
    return {
        'status': 'SUCCESS',
        'output_type': output_type,
        's3_key': output_s3_key,
        'records': record_count
    }


# =============================================================================
# MAIN
# =============================================================================

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'client_id', 'file_id', 'file_type', 'file_date',
        'staging_bucket', 'reference_bucket', 'outputs'
    ])
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    client_id = args['client_id']
    file_id = args['file_id']
    file_type = args['file_type']
    file_date = args['file_date']
    staging_bucket = args['staging_bucket']
    reference_bucket = args['reference_bucket']
    outputs = json.loads(args['outputs'])
    
    log_info("=" * 70)
    log_info("ITX-CALCULATE (PySpark) - STARTING")
    log_info("=" * 70)
    log_info(f"Client ID:        {client_id}")
    log_info(f"File ID:          {file_id}")
    log_info(f"File Type:        {file_type}")
    log_info(f"File Date:        {file_date}")
    log_info(f"Staging Bucket:   {staging_bucket}")
    log_info(f"Reference Bucket: {reference_bucket}")
    log_info(f"Outputs:          {len(outputs)}")
    log_info("=" * 70)
    
    # Cargar datos del cliente
    log_info(f"Loading client data: {client_id}")
    client_data = get_client_data(client_id)
    
    # Convertir file_date
    try:
        file_date_obj = datetime.strptime(file_date, "%Y-%m-%d").date()
    except ValueError:
        file_date_obj = date.today()
    
    # Cargar tablas de referencia
    log_info("Loading reference tables...")
    ardef, ardef_pd = load_visa_ardef(reference_bucket, file_date_obj)
    country_df = load_country_table(reference_bucket).cache()
    currency_df = load_currency_table(reference_bucket).cache()
    
    # Procesar cada output
    results = []
    total_records = 0
    
    for output_config in outputs:
        output_type = output_config.get('output_type', 'UNKNOWN')
        log_info("")
        log_info("=" * 60)
        log_info(f"Processing: {output_type}")
        log_info("=" * 60)
        
        result = process_output(
            output_config=output_config,
            staging_bucket=staging_bucket,
            reference_bucket=reference_bucket,
            file_type=file_type,
            file_date=file_date,
            client_data=client_data,
            ardef=ardef,
            ardef_pd=ardef_pd,
            country_df=country_df,
            currency_df=currency_df
        )
        
        results.append(result)
        total_records += result.get('records', 0)
        log_info(f"  ✓ {output_type}: {result.get('records', 0):,} records")
    
    log_info("")
    log_info("=" * 70)
    log_info("CALCULATE PROCESS COMPLETED")
    log_info("=" * 70)
    log_info(f"Total outputs: {len(results)}")
    log_info(f"Total records: {total_records:,}")
    
    output_data = {
        'status': 'SUCCESS',
        'total_outputs': len(results),
        'total_records': total_records,
        'outputs': results
    }
    
    log_info(f"Output: {json.dumps(output_data)}")

    ardef.unpersist()
    country_df.unpersist()
    currency_df.unpersist()

    job.commit()
    
    return output_data


if __name__ == "__main__":
    main()
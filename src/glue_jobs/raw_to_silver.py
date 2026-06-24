"""
PROYECTO: Data Lake Académico
MÓDULO: raw_to_silver.py
DESCRIPCIÓN: Job de Glue para transformar datos desde la capa RAW (Bronze) a SILVER.
            Realiza normalización dinámica de nombres de columnas, tipificación, 
            limpieza de caracteres corruptos y enriquecimiento de metadatos técnicos.
"""

import io
import logging
import re
import sys
from datetime import datetime
from typing import List, Optional

import boto3
import pandas as pd
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit,
    regexp_extract, upper, trim, when
)

# -----------------------------------------------------------------------------
# DICCIONARIO MAESTRO SISBÉN
# -----------------------------------------------------------------------------
# Traducción de códigos técnicos a nombres de negocio para facilitar el análisis.
SISBEN_MAP = {
    "cod_mpio": "municipio_codigo",
    "h_5": "indicador_pobreza_ipm",
    "i1": "priv_bajo_logro_educativo",
    "i2": "priv_analfabetismo",
    "i3": "priv_inasistencia_escolar",
    "i4": "priv_rezago_escolar",
    "i5": "priv_barreras_cuidado_infancia",
    "i6": "priv_trabajo_infantil",
    "i7": "priv_desempleo_larga_duracion",
    "i8": "priv_trabajo_informal",
    "i9": "priv_sin_aseguramiento_salud",
    "i10": "priv_barreras_acceso_salud",
    "i11": "priv_sin_acceso_agua_mejorada",
    "i12": "priv_eliminacion_excretas_inadecuada",
    "i13": "priv_material_pisos_inadecuado",
    "i14": "priv_material_paredes_inadecuado",
    "i15": "priv_hacinamiento_critico",
    "grupo": "sisben_grupo",
    "nivel": "sisben_nivel",
    "clasificacion": "sisben_clasificacion",
    "zona": "sisben_zona",
    "per001": "pers_sexo",
    "per002": "pers_edad",
    "per003": "pers_parentesco_jefe",
    "per004": "pers_estado_civil",
    "per007": "pers_seguridad_social",
    "per011": "pers_embarazada",
    "per015": "pers_sabe_leer_escribir",
    "per016": "pers_estudia_actualmente",
    "per017": "pers_nivel_educativo",
    "per018": "pers_cotiza_pension",
    "per019": "pers_actividad_principal_mes",
    "per020": "pers_posicion_ocupacional"
}

# -----------------------------------------------------------------------------
# CONFIGURACIÓN DE PARÁMETROS Y AWS
# -----------------------------------------------------------------------------
ARG_NAMES = ["JOB_NAME", "RAW_BUCKET", "SILVER_BUCKET", "SILVER_DATABASE"]
args = getResolvedOptions(sys.argv, ARG_NAMES)

RAW_BUCKET = args["RAW_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]
SILVER_DATABASE = args["SILVER_DATABASE"]
JOB_NAME = args["JOB_NAME"]

# Mapeo de Datasets Gubernamentales (Socrata IDs oficiales)
GOBIERNO_TABLE_MAP = {
    "hq2v-5umk": "gobierno_sisben_iv",
    "n48w-gutb": "gobierno_internet_fijo",
    "ji8i-4anb": "gobierno_cobertura_educativa"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger("raw_to_silver")

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Optimizaciones de rendimiento para la escritura en formato Parquet
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "16")

RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
s3_client = boto3.client("s3")

# -----------------------------------------------------------------------------
# COMPONENTES UTILITARIOS (HELPERS)
# -----------------------------------------------------------------------------
def normalize_column_name(name: str) -> str:
    """Estandariza los nombres de las columnas eliminando tildes y caracteres especiales."""
    s = str(name).strip().lower()
    s = re.sub(r"[áäâà]", "a", s); s = re.sub(r"[éëêè]", "e", s)
    s = re.sub(r"[íïîì]", "i", s); s = re.sub(r"[óöôò]", "o", s)
    s = re.sub(r"[úüûù]", "u", s); s = s.replace("ñ", "n")
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    return s.strip("_")

def add_silver_metadata(df: DataFrame, source: str) -> DataFrame:
    """Inyecta columnas técnicas de auditoría para garantizar la trazabilidad del linaje."""
    return df.withColumn("_silver_source", lit(source)) \
             .withColumn("_silver_processed_at", current_timestamp()) \
             .withColumn("_silver_run_id", lit(RUN_ID))

def write_silver_table(df: DataFrame, table_name: str):
    """Aplica normalización snake_case final y materializa los datos en S3 (Parquet)."""
    output_path = f"s3://{SILVER_BUCKET}/{table_name}/"
    cols = [normalize_column_name(c) for c in df.columns]
    df.toDF(*cols).coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info(f"==> Tabla Silver guardada exitosamente: {table_name}")

# -----------------------------------------------------------------------------
# PROCESAMIENTO: FUENTE UNAD
# -----------------------------------------------------------------------------
def process_unad():
    """Carga y procesa la información de estudiantes de la UNAD extrayendo el período."""
    logger.info("--- INICIANDO FUENTE: UNAD ---")
    path = f"s3://{RAW_BUCKET}/unad/*.csv"
    try:
        df = spark.read.option("sep", ";").option("header", "true").csv(path)
        df = df.withColumn("_source_file", input_file_name()) \
               .withColumn("_periodo_codigo", regexp_extract(col("_source_file"), r"estudiantes_(.+)\.csv", 1))

        logger.info(f"UNAD: Registros cargados: {df.count()}")
        write_silver_table(add_silver_metadata(df, "unad"), "unad_estudiantes")
    except Exception as e:
        logger.error(f"Error procesando UNAD: {e}")

# -----------------------------------------------------------------------------
# PROCESAMIENTO: FUENTE DATOS GOBIERNO
# -----------------------------------------------------------------------------
def process_gobierno():
    """Procesa los datasets de origen JSON aplicando diccionarios de traducción de negocio."""
    logger.info("--- INICIANDO FUENTE: GOBIERNO (Filtrado) ---")

    response = s3_client.list_objects_v2(Bucket=RAW_BUCKET, Prefix="gobierno/")

    for obj in response.get("Contents", []):
        if not obj["Key"].endswith(".json"): continue

        ds_id = obj["Key"].split("/")[-1].replace("dataset_", "").replace(".json", "")

        if ds_id not in GOBIERNO_TABLE_MAP:
            logger.info(f"Saltando dataset no mapeado: {ds_id}")
            continue

        table_name = GOBIERNO_TABLE_MAP[ds_id]
        logger.info(f"Gobierno: Procesando {ds_id} -> {table_name}")
        df = spark.read.option("multiLine", "true").json(f"s3://{RAW_BUCKET}/{obj['Key']}")

        if ds_id == "hq2v-5umk":
            for old, new in SISBEN_MAP.items():
                if old in df.columns:
                    df = df.withColumnRenamed(old, new)

        write_silver_table(add_silver_metadata(df, "gobierno"), table_name)

# -----------------------------------------------------------------------------
# PROCESAMIENTO: FUENTE SPADIES (MINEDUCACIÓN)
# -----------------------------------------------------------------------------
def process_spadies():
    """Ejecuta proceso de Unpivot (Melt) y estandariza estados de apoyo financiero."""
    logger.info("--- INICIANDO FUENTE: SPADIES (Limpieza Avanzada de Strings) ---")
    response = s3_client.list_objects_v2(Bucket=RAW_BUCKET, Prefix="spadies/")

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".csv"): continue

        logger.info(f"SPADIES: Transformando archivo {key}")
        body = s3_client.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
        
        text_content = body.decode("utf-8", errors="replace")
        pdf = pd.read_csv(io.StringIO(text_content), sep=";", dtype=str)

        id_col = pdf.columns[0]
        long = pdf.melt(id_vars=[id_col], var_name="periodo_academico", value_name="valor_str")

        df = spark.createDataFrame(long)
        id_norm = normalize_column_name(id_col)
        
        # Homologación de categorías financieras para la reportería analítica posterior
        df = df.withColumn(
            id_norm,
            when(col(id_col).like("%No recibi%"), lit("No recibio credito"))
            .when(col(id_col).like("%Largo%"), lit("Largo plazo"))
            .when(col(id_col).like("%Mediano%"), lit("Mediano plazo"))
            .otherwise(col(id_col))
        )

        table_name = f"spadies_{normalize_column_name(key.split('/')[-1].split('.')[0])}"
        write_silver_table(add_silver_metadata(df, table_name), table_name)

# -----------------------------------------------------------------------------
# PROCESAMIENTO: FUENTE SNIES (GRADUADOS)
# -----------------------------------------------------------------------------
def process_snies():
    """Procesa reportes en formato Excel omitiendo de forma dinámica filas institucionales."""
    logger.info("--- INICIANDO FUENTE: SNIES (Detección Dinámica para Crawler) ---")

    response = s3_client.list_objects_v2(Bucket=RAW_BUCKET, Prefix="snies/")

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if not key.lower().endswith((".xlsx", ".xlsb")): continue

        try:
            logger.info(f"SNIES: Cargando archivo desde S3: {key}")
            file_obj = s3_client.get_object(Bucket=RAW_BUCKET, Key=key)
            content = file_obj["Body"].read()

            # Se establece header=5 para apuntar dinámicamente a la cabecera real del reporte
            pdf = pd.read_excel(io.BytesIO(content), engine='openpyxl', header=5, dtype=str)
            pdf = pdf.dropna(how="all").fillna("")

            logger.info(f"SNIES: Columnas detectadas dinámicamente: {list(pdf.columns[:3])}")

            df = spark.createDataFrame(pdf)
            write_silver_table(add_silver_metadata(df, "snies"), "snies_graduados")

        except Exception as e:
            logger.error(f"Error procesando el archivo SNIES {key}: {str(e)}")

# -----------------------------------------------------------------------------
# ENTRADA PRINCIPAL DEL PIPELINE
# -----------------------------------------------------------------------------
def main():
    logger.info(f"--- INICIANDO EJECUCIÓN JOB: {JOB_NAME} ---")

    job = Job(glue_context)
    job.init(JOB_NAME, args)

    process_unad()
    process_gobierno()
    process_spadies()
    process_snies()

    job.commit()
    logger.info(f"--- EJECUCIÓN FINALIZADA SATISFACTORIAMENTE ---")

if __name__ == "__main__":
    main()
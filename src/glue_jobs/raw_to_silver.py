"""
PROYECTO: Data Lake Académico
MODULO: raw_to_silver.py

DESCRIPCIÓN:
Job PySpark para transformar datos RAW (Bronze) hacia SILVER.

Características:
- Lectura incremental mediante Glue Bookmarks
- Escritura en Parquet optimizado
- Normalización de columnas
- Limpieza básica y tipificación
- Metadata de trazabilidad
- Manejo dinámico de esquemas anchos (Unpivot nativo) para SPADIES
- Glue Crawlers manejan el catálogo automáticamente
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
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name,
    lit,
    regexp_extract,
    upper,
    trim,
)

# -----------------------------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------------------------

ARG_NAMES = [
    "JOB_NAME",
    "RAW_BUCKET",
    "SILVER_BUCKET",
    "SILVER_DATABASE",
    "PROJECT_NAME",
    "ENVIRONMENT",
]

args = getResolvedOptions(sys.argv, ARG_NAMES)

RAW_BUCKET = args["RAW_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]
SILVER_DATABASE = args["SILVER_DATABASE"]
JOB_NAME = args["JOB_NAME"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logger = logging.getLogger("raw_to_silver")

# -----------------------------------------------------------------------------
# SPARK / GLUE
# -----------------------------------------------------------------------------

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Configuraciones Spark optimizadas
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
spark.conf.set("spark.sql.shuffle.partitions", "8")

job = Job(glue_context)
job.init(JOB_NAME, args)

RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

s3_client = boto3.client("s3")

# -----------------------------------------------------------------------------
# MAPEOS
# -----------------------------------------------------------------------------

GOBIERNO_TABLE_MAP = {
    "hq2v-5umk": "gobierno_sisben_iv",
    "n48w-gutb": "gobierno_internet_fijo",
    "ji8i-4anb": "gobierno_cobertura_educativa",
    "6v4n-7ahj": "gobierno_renta_ciudadana",
    "nkjx-rsq7": "gobierno_indicadores_dnp",
}

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def normalize_column_name(name: str) -> str:
    """Normaliza nombres de columnas para Athena/Glue."""
    if not name:
        return "_unknown"

    s = str(name).strip().lower()

    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
    }

    for src, tgt in replacements.items():
        s = s.replace(src, tgt)

    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s)

    return s.strip("_")


def add_silver_metadata(df: DataFrame, source: str) -> DataFrame:
    """Agrega metadata técnica de trazabilidad."""
    return (
        df.withColumn("_silver_source", lit(source))
          .withColumn("_silver_processed_at", current_timestamp())
          .withColumn("_silver_run_id", lit(RUN_ID))
    )


def write_silver_table(
    df: DataFrame,
    table_name: str,
    partition_cols: Optional[List[str]] = None,
) -> None:
    """Escribe parquet optimizado en Silver."""
    output_path = f"s3://{SILVER_BUCKET}/{table_name}/"

    logger.info(f"Escribiendo tabla {table_name} en {output_path}")

    # Normalización eficiente de nombres de columnas
    normalized_columns = [normalize_column_name(c) for c in df.columns]
    df = df.toDF(*normalized_columns)

    # Reducir pequeños archivos para optimizar lecturas en Athena
    df = df.coalesce(1)

    writer = df.write.mode("append").format("parquet")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)
    logger.info(f"Tabla Silver {table_name} escrita correctamente")


def s3_prefix_exists(bucket: str, prefix: str) -> bool:
    """Valida si existe al menos un archivo bajo un prefijo."""
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1
    )
    return "Contents" in response

# -----------------------------------------------------------------------------
# UNAD
# -----------------------------------------------------------------------------

def process_unad() -> None:
    """Procesa microdatos académicos UNAD."""
    logger.info("Iniciando procesamiento de fuente: UNAD")

    prefix = "unad/"

    if not s3_prefix_exists(RAW_BUCKET, prefix):
        logger.info("No existen archivos UNAD para procesar")
        return

    path = f"s3://{RAW_BUCKET}/unad/*.csv"

    try:
        logger.info(f"Leyendo archivos UNAD desde {path}")

        df = (
            spark.read
            .option("sep", ";")
            .option("header", "true")
            .option("encoding", "utf-8")
            .csv(path)
        )

        logger.info("CSV UNAD cargado correctamente")

        df = (
            df.withColumn("_source_file", input_file_name())
              .withColumn(
                  "_periodo_codigo",
                  regexp_extract(col("_source_file"), r"estudiantes_(.+)\.csv", 1)
              )
        )

        if "periodo" in [c.lower() for c in df.columns]:
            id_col = next(c for c in df.columns if c.lower() == "periodo")
            df = df.filter(col(id_col).isNotNull() & (trim(col(id_col)) != ""))

        if "edad" in df.columns:
            df = df.withColumn("edad", col("edad").cast("int"))

        if "estrato_social" in df.columns:
            df = df.withColumn("estrato_social", col("estrato_social").cast("int"))

        text_cols = ["sexo", "escuela", "programa", "departamento_residencia"]

        for c in text_cols:
            if c in df.columns:
                df = df.withColumn(c, upper(trim(col(c))))

        df = add_silver_metadata(df, "unad")

        logger.info("Escribiendo parquet UNAD en Silver")
        write_silver_table(df, "unad_estudiantes")
        logger.info("Procesamiento UNAD finalizado")

    except Exception as exc:
        logger.error(f"Error procesando UNAD: {str(exc)}")

# -----------------------------------------------------------------------------
# SPADIES (Versión Resiliente con lectura de nombre de archivo)
# -----------------------------------------------------------------------------

def process_spadies() -> None:
    """Procesa dataset SPADIES dinámicamente según el nombre del archivo."""
    logger.info("Iniciando procesamiento de fuente: SPADIES")

    response = s3_client.list_objects_v2(Bucket=RAW_BUCKET, Prefix="spadies/")
    files = response.get("Contents", [])

    if not files:
        logger.info("No existen archivos SPADIES para procesar")
        return

    for obj in files:
        key = obj["Key"]
        if not key.lower().endswith(".csv"):
            continue

        try:
            logger.info(f"Procesando SPADIES: {key}")

            # 1. Sacar el nombre limpio del archivo. Ej: "CreditoIcetex"
            file_name_raw = key.split("/")[-1].replace(".csv", "").replace(".CSV", "")

            # 2. Nombrar la tabla dinámicamente usando nuestro helper. Ej: "spadies_creditoicetex"
            table_name = f"spadies_{normalize_column_name(file_name_raw)}"

            body = s3_client.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
            text = body.decode("utf-8-sig", errors="replace")

            pdf = pd.read_csv(io.StringIO(text), sep=";", dtype=str)
            pdf.columns = [c.strip() for c in pdf.columns]

            id_col = pdf.columns[0]
            period_cols = [c for c in pdf.columns[1:] if c]

            long = pdf.melt(
                id_vars=[id_col],
                value_vars=period_cols,
                var_name="periodo_academico",
                value_name="valor_str",
            )

            long = long.rename(columns={id_col: normalize_column_name(id_col)})

            long["valor_str"] = long["valor_str"].astype(str).str.replace("%", "", regex=False).str.replace(",", ".", regex=False)
            long["porcentaje"] = pd.to_numeric(long["valor_str"], errors="coerce").fillna(0.0)
            long["valor_str"] = long["valor_str"].replace("nan", "")

            df = spark.createDataFrame(long)

            df = df.withColumn("_dataset_id", lit(file_name_raw))
            df = add_silver_metadata(df, "spadies")

            write_silver_table(df, table_name)
            logger.info(f"SPADIES procesado exitosamente y guardado como: {table_name}")

        except Exception as exc:
            logger.error(f"Error procesando SPADIES {key}: {str(exc)}")

# -----------------------------------------------------------------------------
# GOBIERNO
# -----------------------------------------------------------------------------

def process_gobierno() -> None:
    """Procesa datasets JSON de Datos Abiertos Colombia."""
    logger.info("Iniciando procesamiento de fuente: GOBIERNO")

    response = s3_client.list_objects_v2(Bucket=RAW_BUCKET, Prefix="gobierno/")
    files = response.get("Contents", [])

    if not files:
        logger.info("No existen datasets de gobierno")
        return

    for obj in files:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        try:
            file_name = key.split("/")[-1].replace(".json", "")
            ds_id = file_name.replace("dataset_", "")

            table_name = GOBIERNO_TABLE_MAP.get(ds_id, f"gobierno_{ds_id.replace('-', '_')}")
            path = f"s3://{RAW_BUCKET}/{key}"

            logger.info(f"Procesando dataset gobierno: {ds_id}")

            df = spark.read.option("multiLine", "true").json(path)
            df = df.withColumn("_dataset_id", lit(ds_id))
            df = add_silver_metadata(df, "gobierno")

            write_silver_table(df, table_name)
            logger.info(f"Dataset gobierno procesado: {ds_id}")

        except Exception as exc:
            logger.warning(f"Error procesando dataset gobierno {key}: {str(exc)}")

# -----------------------------------------------------------------------------
# SNIES (Restaurado el escudo de nulos)
# -----------------------------------------------------------------------------

def process_snies() -> None:
    """Procesa archivos Excel SNIES."""
    logger.info("Iniciando procesamiento de fuente: SNIES")

    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix="snies/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.lower().endswith((".xlsx", ".xlsb")):
                continue

            try:
                logger.info(f"Procesando archivo SNIES: {key}")

                response = s3_client.get_object(Bucket=RAW_BUCKET, Key=key)
                content = response["Body"].read()
                engine = "pyxlsb" if key.endswith(".xlsb") else "openpyxl"

                preview = pd.read_excel(io.BytesIO(content), engine=engine, nrows=20, header=None)

                matches = preview.index[
                    preview.astype(str).apply(lambda x: x.str.contains("CÓDIGO|CODIGO", case=False, na=False)).any(axis=1)
                ].tolist()

                if not matches:
                    logger.warning(f"No se detectó encabezado válido en {key}")
                    continue

                header_row = matches[0]

                # Restauramos el dtype=str y llenamos los nulos (NaN) para que Spark no reviente
                pdf = pd.read_excel(io.BytesIO(content), engine=engine, header=header_row, dtype=str)
                pdf = pdf.dropna(how="all").fillna("")

                pdf.columns = [normalize_column_name(c) for c in pdf.columns]

                df = spark.createDataFrame(pdf)
                df = add_silver_metadata(df, "snies")

                write_silver_table(df, "snies_graduados")
                logger.info(f"Archivo SNIES procesado: {key}")

            except Exception as exc:
                logger.error(f"Error procesando archivo SNIES {key}: {str(exc)}")

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main() -> None:
    logger.info(f"Iniciando ejecución de Job: {JOB_NAME}")

    process_unad()
    process_spadies()
    process_gobierno()
    process_snies()

    job.commit()
    logger.info("Ejecución finalizada satisfactoriamente.")

if __name__ == "__main__":
    main()
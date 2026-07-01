"""
PROYECTO: Data Lake Académico
MÓDULO: silver_to_gold.py
DESCRIPCIÓN: Materialización de la capa Gold (Analítica).
            Enriquece microdatos de la UNAD con variables socioeconómicas del Sisbén
            y conectividad de MinTIC para el modelo de predicción de deserción.
"""

import logging
import sys
from datetime import datetime
from typing import List, Optional

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast, col, current_timestamp, lead, lit, when, count, avg, upper, trim
)
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# CONFIGURACIÓN DE ARGUMENTOS
# -----------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_DATABASE", "GOLD_DATABASE", "GOLD_BUCKET"])
SILVER_DATABASE = args["SILVER_DATABASE"]
GOLD_DATABASE = args["GOLD_DATABASE"]
GOLD_BUCKET = args["GOLD_BUCKET"]
JOB_NAME = args["JOB_NAME"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger("silver_to_gold")

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(JOB_NAME, args)

RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

# -----------------------------------------------------------------------------
# FUNCIONES UTILITARIAS
# -----------------------------------------------------------------------------
def safe_spark_table(db: str, table: str) -> Optional[DataFrame]:
    """Carga una tabla de Glue de forma segura."""
    try:
        return spark.table(f"{db}.{table}")
    except Exception:
        logger.warning(f"No se pudo cargar la tabla {db}.{table}")
        return None

def write_gold_clean(df: DataFrame, table_name: str, partition_cols: Optional[List[str]] = None) -> None:
    """Escribe en S3 en formato Parquet de forma atómica sin registrar en el catálogo."""
    output_path = f"s3://{GOLD_BUCKET}/{table_name}/"

    # Auditoría técnica
    df_final = df.withColumn("_gold_processed_at", current_timestamp()) \
                 .withColumn("_gold_run_id", lit(RUN_ID))

    writer = df_final.write.mode("overwrite").format("parquet")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)
    logger.info(f"Datos Gold escritos en S3: {table_name}")

# -----------------------------------------------------------------------------
# 1. CAPA RAW GOLD (Espejo de Silver para Auditoría)
# -----------------------------------------------------------------------------
def build_raw_gold_layers():
    """Mantiene versiones íntegras de las dimensiones originales en Gold."""
    logger.info("Generando capas Raw Gold para auditoría y BI...")
    tables = {
        "snies_graduados": "dim_gold_snies_graduados",
        "spadies_creditoicetex": "dim_gold_spadies_creditos",
        "gobierno_sisben_iv": "dim_gold_gobierno_sisben",
        "gobierno_internet_fijo": "dim_gold_gobierno_internet"
    }
    for silver_name, gold_name in tables.items():
        df = safe_spark_table(SILVER_DATABASE, silver_name)
        if df:
            write_gold_clean(df, gold_name)

# -----------------------------------------------------------------------------
# 2. CAPA ANALYTICAL GOLD (Consolidado para Modelado de Datos)
# -----------------------------------------------------------------------------
def build_analytical_gold():
    """Crea la tabla maestra enriquecida para el modelo de Machine Learning."""
    logger.info("Construyendo fact_estudiante_periodo enriquecida...")

    unad = safe_spark_table(SILVER_DATABASE, "unad_estudiantes")
    sisben = safe_spark_table(SILVER_DATABASE, "gobierno_sisben_iv")
    mintic = safe_spark_table(SILVER_DATABASE, "gobierno_internet_fijo")

    if not (unad and sisben and mintic):
        logger.error("Faltan fuentes en Silver para completar el enriquecimiento.")
        return

    # A. TRADUCTOR DE MUNICIPIOS BLINDADO (Mapeo Nombre + Departamento -> Código DIVIPOLA)
    # Incluye departamento para mitigar duplicados por municipios homónimos en distintas regiones
    municipio_ref = mintic.select(
        upper(trim(col("municipio"))).alias("nombre_mpio_ref"),
        upper(trim(col("departamento"))).alias("nombre_depto_ref"),
        col("cod_municipio").alias("codigo_divipola")
    ).distinct()

    # B. MÉTRICAS SOCIOECONÓMICAS (Agregación Sisbén con nombres claros)
    sisben_metrics = sisben.groupBy("municipio_codigo").agg(
        avg("priv_trabajo_informal").alias("tasa_informalidad_mpio"),
        avg("priv_hacinamiento_critico").alias("tasa_hacinamiento_mpio"),
        avg("indicador_pobreza_ipm").alias("promedio_ipm_mpio"),
        count("*").alias("poblacion_sisben_mpio")
    )

    # C. LÓGICA DE DESERCIÓN CRONOLÓGICA (Alineación exacta con prefijo 'periodo_')
    periods = spark.createDataFrame([
        ("periodo_1701", 2024, 1, 1), ("periodo_1702", 2024, 2, 2), 
        ("periodo_1703", 2024, 3, 3), ("periodo_1704", 2024, 4, 4), 
        ("periodo_1705", 2024, 5, 5), ("periodo_2031", 2025, 1, 6),
        ("periodo_2032", 2025, 2, 7), ("periodo_2033", 2025, 3, 8), 
        ("periodo_2034", 2025, 4, 9), ("periodo_2035", 2025, 5, 10)
    ], schema="periodo_codigo string, anio int, semestre int, periodo_orden int")

    # Unimos UNAD con el orden cronológico de periodos usando inner join
    fact = unad.withColumnRenamed("_periodo_codigo", "periodo_codigo") \
               .join(broadcast(periods), on="periodo_codigo", how="inner")

    # Ventana por estudiante para identificar si aparece en algún periodo posterior
    w = Window.partitionBy("id").orderBy("periodo_orden")
    fact = fact.withColumn("next_periodo_orden", lead("periodo_orden").over(w)) \
               .withColumn("desertion_t1",
                    when(col("periodo_orden") >= 10, lit(None).cast("int"))
                    .when(col("next_periodo_orden").isNull(), 1)
                    .otherwise(0))

    # D. EL GRAN CRUCE (Enriquecimiento Final)
    # 1. Normalizamos los nombres de municipio y departamento de residencia en la UNAD
    fact_clean = fact.withColumn("mpio_norm", upper(trim(col("municipio_residencia")))) \
                     .withColumn("depto_norm", upper(trim(col("departamento_residencia"))))

    # 2. Join 1: Obtenemos el Código DIVIPOLA validando la localización completa
    fact_with_code = fact_clean.join(
        broadcast(municipio_ref),
        (fact_clean.mpio_norm == municipio_ref.nombre_mpio_ref) & 
        (fact_clean.depto_norm == municipio_ref.nombre_depto_ref),
        "left"
    )

    # 3. Join 2: Acoplamos las métricas agregadas del Sisbén usando el Código DIVIPOLA unificado
    fact_final = fact_with_code.join(
        broadcast(sisben_metrics),
        fact_with_code.codigo_divipola == sisben_metrics.municipio_codigo,
        "left"
    )

    # Guardamos la tabla Maestra particionada por año para optimizar Athena
    write_gold_clean(fact_final, "fact_estudiante_periodo", partition_cols=["anio"])

# -----------------------------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    logger.info(f"Iniciando Job Gold: {JOB_NAME}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")

    build_raw_gold_layers()
    build_analytical_gold()

    job.commit()
    logger.info("Pipeline Gold finalizado exitosamente.")

if __name__ == "__main__":
    main()
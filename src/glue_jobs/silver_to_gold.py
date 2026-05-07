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

# Configuración de argumentos
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

def safe_spark_table(db: str, table: str) -> Optional[DataFrame]:
    try:
        return spark.table(f"{db}.{table}")
    except Exception:
        logger.warning(f"No se pudo cargar la tabla {db}.{table}")
        return None

def write_gold_clean(df: DataFrame, table_name: str, partition_cols: Optional[List[str]] = None) -> None:
    """Escribe en S3 y registra en Glue con nombres limpios."""
    output_path = f"s3://{GOLD_BUCKET}/{table_name}/"
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_DATABASE}.{table_name}")

    df_final = df.withColumn("_gold_processed_at", current_timestamp()) \
                 .withColumn("_gold_run_id", lit(RUN_ID))

    writer = df_final.write.mode("overwrite").format("parquet").option("path", output_path)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(f"{GOLD_DATABASE}.{table_name}")
    logger.info(f"Escritura finalizada: {table_name}")

# --- 1. CAPA RAW GOLD (Copia exacta de Silver para auditoría) ---
def build_raw_gold_layers():
    logger.info("Generando copias Raw en Gold...")
    tables = {
        "snies_graduados": "dim_raw_snies_graduados",
        "spadies_creditoicetex": "dim_raw_spadies_creditos",
        "gobierno_sisben_iv": "dim_raw_gobierno_sisben",
        "gobierno_internet_fijo": "dim_raw_gobierno_internet"
    }
    for silver_name, gold_name in tables.items():
        df = safe_spark_table(SILVER_DATABASE, silver_name)
        if df:
            write_gold_clean(df, gold_name)

# --- 2. CAPA ANALYTICAL GOLD (La "verdad" para el modelo) ---
def build_analytical_gold():
    logger.info("Construyendo tabla de hechos enriquecida...")

    unad = safe_spark_table(SILVER_DATABASE, "unad_estudiantes")
    sisben = safe_spark_table(SILVER_DATABASE, "gobierno_sisben_iv")
    mintic = safe_spark_table(SILVER_DATABASE, "gobierno_internet_fijo")

    if not (unad and sisben and mintic):
        logger.error("Faltan tablas esenciales para el cruce.")
        return

    # A. Crear Diccionario de Municipios (Nombre -> Código)
    # Usamos MinTIC que tiene ambos campos
    municipio_ref = mintic.select(
        upper(trim(col("municipio"))).alias("nombre_mpio_ref"),
        col("cod_municipio").alias("codigo_divipola")
    ).distinct()

    # B. Agregación de Sisbén (Promedios de pobreza por municipio)
    # Incluimos las variables de privación que pediste para el modelo
    sisben_metrics = sisben.groupBy("cod_mpio").agg(
        avg("i8").alias("tasa_informalidad_mpio"),     # I8: Trabajo informal
        avg("i15").alias("tasa_hacinamiento_mpio"),   # I15: Hacinamiento
        avg("h_5").alias("promedio_ipm_mpio"),         # H_5: Pobreza multidimensional
        count("*").alias("poblacion_sisben_mpio")
    )

    # C. Lógica de Deserción UNAD
    periods = spark.createDataFrame([
        ("1701", 2024, 1, 1), ("1702", 2024, 2, 2), ("1703", 2024, 3, 3), ("1704", 2024, 4, 4), ("1705", 2024, 5, 5),
        ("2031", 2025, 1, 6), ("2032", 2025, 2, 7), ("2033", 2025, 3, 8), ("2034", 2025, 4, 9), ("2035", 2025, 5, 10)
    ], schema="periodo_codigo string, anio int, semestre int, periodo_orden int")

    fact = unad.withColumnRenamed("_periodo_codigo", "periodo_codigo") \
               .join(broadcast(periods), on="periodo_codigo", how="left")

    w = Window.partitionBy("id").orderBy("periodo_orden")
    fact = fact.withColumn("next_periodo_orden", lead("periodo_orden").over(w)) \
               .withColumn("desertion_t1",
                    when(col("periodo_orden") >= 10, lit(None).cast("int"))
                    .when(col("next_periodo_orden").isNull() | (col("next_periodo_orden") > col("periodo_orden") + 1), 1)
                    .otherwise(0))

    # D. EL CRUCE MAESTRO (Traducción y Enriquecimiento)
    # 1. Normalizamos nombre en UNAD
    fact_clean = fact.withColumn("mpio_norm", upper(trim(col("municipio_residencia"))))

    # 2. Pegamos Código DIVIPOLA
    fact_with_code = fact_clean.join(broadcast(municipio_ref), fact_clean.mpio_norm == municipio_ref.nombre_mpio_ref, "left")

    # 3. Pegamos métricas del Sisbén
    fact_final = fact_with_code.join(broadcast(sisben_metrics), fact_with_code.codigo_divipola == sisben_metrics.cod_mpio, "left")

    write_gold_clean(fact_final, "fact_estudiante_periodo", partition_cols=["anio"])

def main():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")
    build_raw_gold_layers()
    build_analytical_gold()
    job.commit()

if __name__ == "__main__":
    main()
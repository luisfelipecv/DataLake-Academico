"""
Glue PySpark job: Silver -> Gold.

Materializes the analytical-ready Gold layer used by Athena (EDA),
the App Runner dashboards and the SageMaker training pipeline.

Tables produced
---------------
- fact_estudiante_periodo
    UNAD microdata at (id, periodo) granularity, enriched with the
    chronological period ordering and a binary `desertion_t1` target
    computed via window over the per-student timeline.
- dim_gobierno_municipio
    Sisbén IV vulnerability (% group D per municipality) and MinTIC
    fixed-internet access aggregated at the municipality level.
- dim_gobierno_cobertura_dpto
    MEN coverage / dropout / pass-rate metrics at the department level.
- dim_snies_graduados_programa
    Total graduates per academic program (proxy for program throughput).
- dim_spadies_creditos_periodo
    ICETEX credit-type distribution by period, already unpivoted in Silver.

Per-table failures are logged but never abort the rest of the pipeline.
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
    broadcast,
    col,
    count,
    countDistinct,
    current_timestamp,
    lead,
    lit,
    regexp_replace,
    sum as spark_sum,
    trim,
    upper,
    when,
)
from pyspark.sql.window import Window

ARG_NAMES = [
    "JOB_NAME",
    "SILVER_BUCKET",
    "GOLD_BUCKET",
    "SILVER_DATABASE",
    "GOLD_DATABASE",
    "PROJECT_NAME",
    "ENVIRONMENT",
]
args = getResolvedOptions(sys.argv, ARG_NAMES)
SILVER_BUCKET = args["SILVER_BUCKET"]
GOLD_BUCKET = args["GOLD_BUCKET"]
SILVER_DATABASE = args["SILVER_DATABASE"]
GOLD_DATABASE = args["GOLD_DATABASE"]
PROJECT_NAME = args["PROJECT_NAME"]
ENVIRONMENT = args["ENVIRONMENT"]
JOB_NAME = args["JOB_NAME"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("silver_to_gold")

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glue_context)
job.init(JOB_NAME, args)

RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

# (periodo_codigo, anio, semestre, periodo_orden) — orden chronological global
PERIOD_ORDER = [
    ("1701", 2024, 1, 1),
    ("1702", 2024, 2, 2),
    ("1703", 2024, 3, 3),
    ("1704", 2024, 4, 4),
    ("1705", 2024, 5, 5),
    ("2031", 2025, 1, 6),
    ("2032", 2025, 2, 7),
    ("2033", 2025, 3, 8),
    ("2034", 2025, 4, 9),
    ("2035", 2025, 5, 10),
]


def safe_silver_table(name: str) -> Optional[DataFrame]:
    try:
        return spark.table(f"{SILVER_DATABASE}.{name}")
    except Exception as exc:
        logger.warning("Silver table not available: %s.%s (%s)", SILVER_DATABASE, name, exc)
        return None


def normalize_text(c):
    return upper(trim(regexp_replace(c, r"\s+", " ")))


def add_gold_metadata(df: DataFrame, source: str) -> DataFrame:
    return (
        df.withColumn("_gold_source", lit(source))
        .withColumn("_gold_processed_at", current_timestamp())
        .withColumn("_gold_run_id", lit(RUN_ID))
    )


def write_gold(df: DataFrame, table_name: str, partition_cols: Optional[List[str]] = None) -> None:
    output_path = f"s3://{GOLD_BUCKET}/{table_name}/"
    writer = (
        df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .option("path", output_path)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{GOLD_DATABASE}.{table_name}")
    logger.info("Wrote gold table %s.%s -> %s", GOLD_DATABASE, table_name, output_path)


# ----------------------------------------------------------------
# fact_estudiante_periodo (target deserción)
# ----------------------------------------------------------------
def build_fact_estudiante_periodo() -> None:
    unad = safe_silver_table("unad_estudiantes")
    if unad is None:
        logger.error("Cannot build fact_estudiante_periodo: silver UNAD missing")
        return
    if "id" not in unad.columns:
        logger.error("UNAD silver lacks 'id' column; cannot build fact_estudiante_periodo")
        return

    period_df = spark.createDataFrame(
        PERIOD_ORDER,
        schema="periodo_codigo string, anio int, semestre int, periodo_orden int",
    )

    enriched = (
        unad.withColumnRenamed("_periodo_codigo", "periodo_codigo")
        .join(broadcast(period_df), on="periodo_codigo", how="left")
    )

    w = Window.partitionBy("id").orderBy("periodo_orden")
    fact = enriched.withColumn("next_periodo_orden", lead("periodo_orden").over(w)).withColumn(
        "desertion_t1",
        when(col("periodo_orden") >= lit(10), lit(None).cast("int"))
        .when(col("next_periodo_orden").isNull(), lit(1).cast("int"))
        .when(col("next_periodo_orden") > col("periodo_orden") + lit(1), lit(1).cast("int"))
        .otherwise(lit(0).cast("int")),
    )

    if "municipio_residencia" in fact.columns:
        fact = fact.withColumn(
            "municipio_residencia_norm", normalize_text(col("municipio_residencia"))
        )
    if "departamento_residencia" in fact.columns:
        fact = fact.withColumn(
            "departamento_residencia_norm", normalize_text(col("departamento_residencia"))
        )

    fact = add_gold_metadata(fact, "unad")
    write_gold(fact, "fact_estudiante_periodo", partition_cols=["anio"])


# ----------------------------------------------------------------
# dim_gobierno_municipio (Sisbén + MinTIC)
# ----------------------------------------------------------------
def build_dim_gobierno_municipio() -> None:
    sisben = safe_silver_table("gobierno_sisben_iv")
    mintic = safe_silver_table("gobierno_internet_fijo")

    if sisben is None and mintic is None:
        logger.warning("No gobierno sources available for dim_gobierno_municipio")
        return

    sisben_agg: Optional[DataFrame] = None
    if sisben is not None and {"cod_mpio", "grupo"}.issubset(set(sisben.columns)):
        sisben_agg = (
            sisben.filter(col("cod_mpio").isNotNull())
            .groupBy(col("cod_mpio").alias("cod_municipio"))
            .agg(
                count("*").alias("sisben_total_registros"),
                spark_sum(when(col("grupo") == "A", 1).otherwise(0)).alias("sisben_grupo_a_count"),
                spark_sum(when(col("grupo") == "B", 1).otherwise(0)).alias("sisben_grupo_b_count"),
                spark_sum(when(col("grupo") == "C", 1).otherwise(0)).alias("sisben_grupo_c_count"),
                spark_sum(when(col("grupo") == "D", 1).otherwise(0)).alias("sisben_grupo_d_count"),
            )
            .withColumn(
                "sisben_pct_grupo_d",
                when(
                    col("sisben_total_registros") > 0,
                    col("sisben_grupo_d_count") / col("sisben_total_registros"),
                ).otherwise(lit(0.0)),
            )
        )

    mintic_agg: Optional[DataFrame] = None
    if mintic is not None and "cod_municipio" in mintic.columns:
        mintic_agg = (
            mintic.filter(col("cod_municipio").isNotNull())
            .groupBy("cod_municipio")
            .agg(
                spark_sum(col("no_de_accesos").cast("long")).alias("mintic_total_accesos"),
                countDistinct("proveedor").alias("mintic_distinct_proveedores"),
                countDistinct("tecnologia").alias("mintic_distinct_tecnologias"),
            )
        )

    if sisben_agg is not None and mintic_agg is not None:
        df = sisben_agg.join(mintic_agg, on="cod_municipio", how="outer")
    elif sisben_agg is not None:
        df = sisben_agg
    elif mintic_agg is not None:
        df = mintic_agg
    else:
        logger.warning("Neither Sisbén nor MinTIC produced an aggregate; skipping dim_gobierno_municipio")
        return

    df = add_gold_metadata(df, "gobierno")
    write_gold(df, "dim_gobierno_municipio")


# ----------------------------------------------------------------
# dim_gobierno_cobertura_dpto
# ----------------------------------------------------------------
def build_dim_gobierno_cobertura_dpto() -> None:
    cob = safe_silver_table("gobierno_cobertura_educativa")
    if cob is None:
        return
    if not {"ano", "departamento"}.issubset(set(cob.columns)):
        logger.warning("cobertura_educativa silver missing required columns; skipping")
        return

    out = cob.select(
        col("ano").cast("int").alias("anio"),
        col("departamento"),
        normalize_text(col("departamento")).alias("departamento_norm"),
        col("cobertura_neta").cast("double").alias("cobertura_neta"),
        col("cobertura_bruta").cast("double").alias("cobertura_bruta"),
        col("desercion").cast("double").alias("desercion_basica"),
        col("aprobacion").cast("double").alias("aprobacion_basica"),
        col("reprobacion").cast("double").alias("reprobacion_basica"),
    )
    out = add_gold_metadata(out, "cobertura")
    write_gold(out, "dim_gobierno_cobertura_dpto", partition_cols=["anio"])


# ----------------------------------------------------------------
# dim_snies_graduados_programa
# ----------------------------------------------------------------
def build_dim_snies_graduados_programa() -> None:
    snies = safe_silver_table("snies_graduados")
    if snies is None:
        return

    program_col = None
    for cand in [
        "programa_academico",
        "programa_acad_mico",
        "programa",
        "programa_de_formaci_n",
    ]:
        if cand in snies.columns:
            program_col = cand
            break
    if not program_col:
        logger.warning("Could not detect 'programa' column in snies_graduados; skipping dim_snies")
        return

    out = (
        snies.filter(col(program_col).isNotNull())
        .groupBy(col(program_col).alias("programa"))
        .agg(count("*").alias("snies_graduados_total"))
        .withColumn("programa_norm", normalize_text(col("programa")))
    )
    out = add_gold_metadata(out, "snies")
    write_gold(out, "dim_snies_graduados_programa")


# ----------------------------------------------------------------
# dim_spadies_creditos_periodo
# ----------------------------------------------------------------
def build_dim_spadies_creditos_periodo() -> None:
    spd = safe_silver_table("spadies_creditos_icetex")
    if spd is None:
        return
    out = add_gold_metadata(spd, "spadies")
    write_gold(out, "dim_spadies_creditos_periodo")


def main() -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")
    builders = [
        ("fact_estudiante_periodo", build_fact_estudiante_periodo),
        ("dim_gobierno_municipio", build_dim_gobierno_municipio),
        ("dim_gobierno_cobertura_dpto", build_dim_gobierno_cobertura_dpto),
        ("dim_snies_graduados_programa", build_dim_snies_graduados_programa),
        ("dim_spadies_creditos_periodo", build_dim_spadies_creditos_periodo),
    ]
    for name, fn in builders:
        try:
            logger.info("=== Building gold: %s ===", name)
            fn()
        except Exception as exc:
            logger.exception("Failed to build %s: %s", name, exc)

    job.commit()


main()

"""
PROYECTO: Data Lake Académico
MÓDULO: silver_to_gold.py
DESCRIPCIÓN: Materialización de la capa Gold (Analítica).
            Enriquece microdatos de la UNAD con variables socioeconómicas del Sisbén
            y conectividad de MinTIC para el modelo de predicción de deserción.
            Produce: dims espejo de silver, dim_divipola (traductor geográfico),
            fact_estudiante_periodo (BI, grano id x periodo) y
            fact_estudiante_semestre (modelado ML, grano id x semestre con target
            desertion_t1 censurado en el último semestre observado).
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
    broadcast, col, current_timestamp, lead, lag, lit, when, count, avg, upper, trim,
    translate, regexp_replace, row_number, dense_rank, coalesce, countDistinct,
    lpad, sum as spark_sum, min as spark_min, max as spark_max
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
def normalize_geo_name(c):
    """Normaliza nombres de municipio/departamento para el cruce UNAD <-> gobierno:
    mayúsculas, sin tildes, sin sufijos entre paréntesis, sin puntuación y sin
    sufijos distritales (D.C. / DC), con espacios colapsados. Debe aplicarse a
    AMBOS lados del join para que la comparación sea simétrica."""
    normalized = upper(trim(c))
    normalized = translate(normalized, "ÁÉÍÓÚÜÑÀÈÌÒÙÂÊÎÔÛ", "AEIOUUNAEIOUAEIOU")
    normalized = regexp_replace(normalized, r"\([^)]*\)", " ")
    normalized = regexp_replace(normalized, r"[^A-Z0-9 ]", " ")
    normalized = regexp_replace(normalized, r"\b(D C|DC|DISTRITO)\b", " ")
    normalized = regexp_replace(normalized, r"\s+", " ")
    return trim(normalized)


def build_analytical_gold():
    """Crea las tablas maestras enriquecidas para BI y Machine Learning.

    Escribe tres tablas Gold:
      - dim_divipola: traductor nombre normalizado -> código DIVIPOLA (auditoría del cruce).
      - fact_estudiante_periodo: grano (id, periodo_codigo), para BI/dashboards.
      - fact_estudiante_semestre: grano (id, semestre), tabla de entrenamiento del
        modelo de deserción (las cohortes paralelas 16-01/16-02/8-03 se colapsan
        al semestre académico al que pertenecen).

    Target desertion_t1 (nivel SEMESTRE, right-censoring explícito):
      1  -> el estudiante NO registra matrícula en el semestre académico siguiente
      0  -> el estudiante SÍ registra matrícula en el semestre siguiente
      NULL -> último semestre observado (sin ventana futura: no hay ground truth)
    """
    logger.info("Construyendo capa analítica Gold (periodo + semestre)...")

    unad = safe_spark_table(SILVER_DATABASE, "unad_estudiantes")
    sisben = safe_spark_table(SILVER_DATABASE, "gobierno_sisben_iv")
    mintic = safe_spark_table(SILVER_DATABASE, "gobierno_internet_fijo")

    if not (unad and sisben and mintic):
        logger.error("Faltan fuentes en Silver para completar el enriquecimiento.")
        return

    # A. TRADUCTOR DE MUNICIPIOS (nombre normalizado + departamento -> código DIVIPOLA)
    # MinTIC trae nombres con acentos correctos y el código DIVIPOLA de 5 dígitos.
    # Zero-pad defensivo (bug clásico DIVIPOLA: "5001" vs "05001") y colapso a UN
    # código por pareja (mpio, depto) para que el join sea 1:1 y no multiplique filas.
    municipio_ref = mintic.select(
        normalize_geo_name(col("municipio")).alias("nombre_mpio_ref"),
        normalize_geo_name(col("departamento")).alias("nombre_depto_ref"),
        lpad(trim(col("cod_municipio")), 5, "0").alias("codigo_divipola")
    ).groupBy("nombre_mpio_ref", "nombre_depto_ref").agg(
        spark_max("codigo_divipola").alias("codigo_divipola")
    )
    write_gold_clean(municipio_ref, "dim_divipola")

    # Municipios cuyo nombre normalizado es único a nivel nacional (fallback de join
    # cuando el departamento de la UNAD no coincide por variantes de escritura).
    # La columna de nombre se renombra para no chocar con la de municipio_ref en el
    # join encadenado (ambos DataFrames comparten linaje -> AMBIGUOUS_SELF_JOIN).
    mpio_unico = municipio_ref.groupBy("nombre_mpio_ref").agg(
        countDistinct("codigo_divipola").alias("n_codigos"),
        spark_max("codigo_divipola").alias("codigo_divipola_unico")
    ).filter(col("n_codigos") == 1) \
     .select(col("nombre_mpio_ref").alias("mpio_unico_nombre"), "codigo_divipola_unico")

    # B. MÉTRICAS SOCIOECONÓMICAS (Agregación Sisbén por municipio, llave zero-padded)
    sisben_metrics = sisben \
        .withColumn("municipio_codigo", lpad(trim(col("municipio_codigo")), 5, "0")) \
        .groupBy("municipio_codigo").agg(
            avg(col("priv_trabajo_informal").cast("double")).alias("tasa_informalidad_mpio"),
            avg(col("priv_hacinamiento_critico").cast("double")).alias("tasa_hacinamiento_mpio"),
            avg(col("indicador_pobreza_ipm").cast("double")).alias("promedio_ipm_mpio"),
            count("*").alias("poblacion_sisben_mpio")
        )

    # C. CALENDARIO ACADÉMICO REAL (verificado contra el preámbulo institucional UNAD):
    # cada código de periodo pertenece a un semestre académico; 16-01/16-04 son las
    # cohortes principales, 16-02/16-05 las secundarias y 8-03 los complementarios.
    periods = spark.createDataFrame([
        ("periodo_1701", 2024, "2024-1", 1, "principal",      1),
        ("periodo_1702", 2024, "2024-1", 1, "secundaria",     2),
        ("periodo_1703", 2024, "2024-1", 1, "complementaria", 3),
        ("periodo_1704", 2024, "2024-2", 2, "principal",      4),
        ("periodo_1705", 2024, "2024-2", 2, "secundaria",     5),
        ("periodo_2031", 2025, "2025-1", 3, "principal",      6),
        ("periodo_2032", 2025, "2025-1", 3, "secundaria",     7),
        ("periodo_2033", 2025, "2025-1", 3, "complementaria", 8),
        ("periodo_2034", 2025, "2025-2", 4, "principal",      9),
        ("periodo_2035", 2025, "2025-2", 4, "secundaria",     10),
    ], schema=("periodo_codigo string, anio int, semestre_academico string, "
               "semestre_orden int, cohorte_tipo string, periodo_orden int"))

    # (La columna en Silver ya se llama periodo_codigo: normalize_column_name quita el "_")
    fact = unad.join(broadcast(periods), on="periodo_codigo", how="inner")

    # Dedup: el raw trae ids repetidos dentro de un mismo periodo (variantes menores
    # de escritura del programa). Debe ir ANTES de cualquier ventana por estudiante.
    fact = fact.dropDuplicates(["id", "periodo_codigo"])

    # D. TARGET DE DESERCIÓN A NIVEL SEMESTRE (panel id x semestre)
    sem_panel = fact.select("id", "semestre_orden").distinct()
    max_sem = sem_panel.agg(spark_max("semestre_orden")).collect()[0][0]
    logger.info(f"Último semestre observado (censurado a NULL): orden {max_sem}")

    w_sem = Window.partitionBy("id").orderBy("semestre_orden")
    sem_panel = sem_panel.withColumn("next_semestre_orden", lead("semestre_orden").over(w_sem)) \
        .withColumn("desertion_t1",
                    when(col("semestre_orden") >= max_sem, lit(None).cast("int"))
                    .when(col("next_semestre_orden").isNull(), lit(1))
                    .when(col("next_semestre_orden") == col("semestre_orden") + 1, lit(0))
                    .otherwise(lit(1)))

    # Historia académica as-of-t (solo usa información <= semestre actual: sin leakage)
    w_hist = Window.partitionBy("id").orderBy("semestre_orden") \
                   .rowsBetween(Window.unboundedPreceding, 0)
    sem_panel = sem_panel.withColumn("semestres_cursados_acum", count("*").over(w_hist)) \
        .withColumn("gap_desde_semestre_anterior",
                    col("semestre_orden") - lag("semestre_orden").over(w_sem)) \
        .withColumn("es_primer_semestre",
                    when(lag("semestre_orden").over(w_sem).isNull(), 1).otherwise(0))

    fact = fact.join(sem_panel, on=["id", "semestre_orden"], how="left")

    # E. EL GRAN CRUCE (Enriquecimiento geográfico y socioeconómico)
    # 1. Normalización simétrica de nombres en el lado UNAD
    fact_clean = fact.withColumn("mpio_norm", normalize_geo_name(col("municipio_residencia"))) \
        .withColumn("depto_norm", normalize_geo_name(col("departamento_residencia")))

    # 2. Join 1: municipio + departamento (evita homónimos entre regiones)
    fact_with_code = fact_clean.join(
        broadcast(municipio_ref),
        (fact_clean.mpio_norm == municipio_ref.nombre_mpio_ref) &
        (fact_clean.depto_norm == municipio_ref.nombre_depto_ref),
        "left"
    )

    # 2b. Fallback: si no hubo match exacto pero el nombre de municipio es único a
    # nivel nacional, usamos su código (recupera casos con departamento mal escrito).
    fact_with_code = fact_with_code.join(
        broadcast(mpio_unico),
        col("mpio_norm") == col("mpio_unico_nombre"),
        "left"
    ).withColumn("codigo_divipola", coalesce(col("codigo_divipola"), col("codigo_divipola_unico"))) \
     .drop("codigo_divipola_unico", "mpio_unico_nombre")

    # 3. Join 2: métricas Sisbén por código DIVIPOLA
    fact_final = fact_with_code.join(
        broadcast(sisben_metrics),
        fact_with_code.codigo_divipola == sisben_metrics.municipio_codigo,
        "left"
    )

    # Métricas de fidelidad histórica (SOLO para BI: usan la historia completa del
    # estudiante, por lo que constituyen leakage si se usan como features del modelo).
    w_student = Window.partitionBy("id")
    fact_final = fact_final.withColumn("total_periodos_matriculados", count(col("id")).over(w_student)) \
        .withColumn("primer_periodo", spark_min(col("periodo_orden")).over(w_student)) \
        .withColumn("ultimo_periodo", spark_max(col("periodo_orden")).over(w_student))

    fact_final = fact_final.withColumn("tasa_permanencia",
                                    col("total_periodos_matriculados") /
                                    (col("ultimo_periodo") - col("primer_periodo") + 1))

    # 4. ENRIQUECIMIENTO DE CONECTIVIDAD (MinTIC)
    # Total de accesos residenciales del trimestre más reciente reportado por
    # municipio (sum, no count). Todo MinTIC es <= 2023 y los periodos académicos
    # son 2024-2025: la feature es un snapshot pre-periodo, sin leakage temporal.
    mintic_res_rows = mintic.filter(col("segmento").contains("RESIDENCIAL")) \
        .withColumn("cod_municipio", lpad(trim(col("cod_municipio")), 5, "0")) \
        .withColumn("anno_int", col("anno").cast("int")) \
        .withColumn("trimestre_int", col("trimestre").cast("int")) \
        .withColumn("accesos_int", col("no_de_accesos").cast("long"))

    w_snap = Window.partitionBy("cod_municipio") \
                   .orderBy(col("anno_int").desc(), col("trimestre_int").desc())
    mintic_res = mintic_res_rows.withColumn("_rk", dense_rank().over(w_snap)) \
        .filter(col("_rk") == 1) \
        .groupBy("cod_municipio").agg(
            spark_sum("accesos_int").alias("total_accesos_res"),
            spark_max("anno_int").alias("anio_corte_conectividad")
        )

    # Bandas de conectividad por terciles entre municipios con accesos > 0
    terciles = mintic_res.filter(col("total_accesos_res") > 0) \
        .approxQuantile("total_accesos_res", [0.33, 0.66], 0.01)
    t1, t2 = (terciles + [0, 0])[:2] if terciles else (0, 0)
    logger.info(f"Terciles de conectividad municipal: t1={t1}, t2={t2}")

    fact_final = fact_final.join(
        broadcast(mintic_res),
        fact_final.codigo_divipola == mintic_res.cod_municipio,
        "left"
    ).fillna(0, subset=["total_accesos_res"]) \
    .withColumn("nivel_conectividad",
                when(col("total_accesos_res") == 0, 0)   # Sin dato / sin accesos
                .when(col("total_accesos_res") < t1, 1)  # Baja
                .when(col("total_accesos_res") < t2, 2)  # Media
                .otherwise(3))                           # Alta

    # Métricas de control del cruce (quedan en los logs del job para validación)
    total_rows = fact_final.count()
    matched = fact_final.filter(col("codigo_divipola").isNotNull()).count()
    logger.info(f"Cobertura DIVIPOLA: {matched}/{total_rows} = {matched / max(total_rows, 1):.1%}")

    # ---------------------------------------------------
    # Tabla BI: grano (id, periodo), particionada por año para optimizar Athena
    write_gold_clean(fact_final, "fact_estudiante_periodo", partition_cols=["anio"])

    # ---------------------------------------------------
    # F. TABLA DE MODELADO: fact_estudiante_semestre (grano id x semestre).
    # Si el estudiante aparece en varias cohortes del mismo semestre, se toman los
    # atributos de la cohorte principal (16-01/16-04), luego secundaria, luego compl.
    cohorte_rank = when(col("cohorte_tipo") == "principal", 1) \
        .when(col("cohorte_tipo") == "secundaria", 2).otherwise(3)
    w_pick = Window.partitionBy("id", "semestre_orden") \
                   .orderBy(cohorte_rank.asc(), col("periodo_orden").asc())

    n_periodos_sem = fact_final.groupBy("id", "semestre_orden") \
        .agg(count("*").alias("n_periodos_en_semestre"))

    fact_sem = fact_final.withColumn("_pick", row_number().over(w_pick)) \
        .filter(col("_pick") == 1).drop("_pick") \
        .join(n_periodos_sem, on=["id", "semestre_orden"], how="left") \
        .select(
            "id", "anio", "semestre_academico", "semestre_orden", "cohorte_tipo",
            "n_periodos_en_semestre",
            # Atributos individuales (features del modelo)
            "edad", "sexo", "estrato_social", "zona_de_residencia",
            "escuela", "programa", "zona", "centro",
            "departamento_residencia", "municipio_residencia",
            # Cruce geográfico y socioeconómico
            "codigo_divipola", "tasa_informalidad_mpio", "tasa_hacinamiento_mpio",
            "promedio_ipm_mpio", "poblacion_sisben_mpio",
            "total_accesos_res", "nivel_conectividad", "anio_corte_conectividad",
            # Historia académica as-of-t (sin leakage)
            "semestres_cursados_acum", "gap_desde_semestre_anterior", "es_primer_semestre",
            # Target
            "desertion_t1"
        )

    write_gold_clean(fact_sem, "fact_estudiante_semestre", partition_cols=["anio"])

    # Prevalencia del target por semestre (validación del fix de censura en logs)
    prevalencia = fact_sem.groupBy("semestre_orden").agg(
        count("*").alias("n"),
        avg(col("desertion_t1").cast("double")).alias("tasa_desercion")
    ).orderBy("semestre_orden").collect()
    for row in prevalencia:
        tasa = f"{row['tasa_desercion']:.1%}" if row["tasa_desercion"] is not None else "NULL (censurado)"
        logger.info(f"Semestre orden {row['semestre_orden']}: n={row['n']}, deserción={tasa}")

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
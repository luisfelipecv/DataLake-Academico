"""
Glue PySpark job: Bronze (raw) -> Silver.

Reads heterogeneous sources from the raw bucket, normalizes them and
materializes Silver Parquet tables registered in the Glue Data Catalog:

- UNAD CSV (Windows-1252, two header lines + ';' separator)
- Datos Abiertos Colombia JSON (Socrata API, paginated)
- SNIES Excel (xlsx + xlsb, header row varies between files)
- SPADIES CSV (UTF-8 BOM, pivot/wide layout that we unpivot to long)

Per-source failures are logged but never abort the rest of the pipeline.
"""
import io
import logging
import re
import sys
from datetime import datetime
from typing import Dict, List, Optional

import boto3
import pandas as pd
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, trim, upper, when

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
PROJECT_NAME = args["PROJECT_NAME"]
ENVIRONMENT = args["ENVIRONMENT"]
JOB_NAME = args["JOB_NAME"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("raw_to_silver")

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glue_context)
job.init(JOB_NAME, args)

RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
s3_client = boto3.client("s3")

GOBIERNO_TABLE_MAP = {
    "hq2v-5umk": "gobierno_sisben_iv",
    "n48w-gutb": "gobierno_internet_fijo",
    "ji8i-4anb": "gobierno_cobertura_educativa",
    "6v4n-7ahj": "gobierno_renta_ciudadana",
    "nkjx-rsq7": "gobierno_indicadores_dnp",
}


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------
def normalize_column_name(name) -> str:
    if name is None:
        return "_unknown"
    s = str(name).strip().lower()
    accents = str.maketrans("áéíóúñ", "aeioun")
    s = s.translate(accents)
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "_unknown"


def list_s3_objects(prefix: str, suffix_filter: tuple = ()) -> List[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: List[str] = []
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if not suffix_filter or key.lower().endswith(suffix_filter):
                keys.append(key)
    return keys


def get_s3_bytes(key: str) -> bytes:
    return s3_client.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()


def add_silver_metadata(df: DataFrame, source: str) -> DataFrame:
    return (
        df.withColumn("_silver_source", lit(source))
        .withColumn("_silver_processed_at", current_timestamp())
        .withColumn("_silver_run_id", lit(RUN_ID))
    )


def write_silver_table(df: DataFrame, table_name: str, partition_cols: Optional[List[str]] = None) -> None:
    output_path = f"s3://{SILVER_BUCKET}/{table_name}/"
    writer = (
        df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .option("path", output_path)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{SILVER_DATABASE}.{table_name}")
    logger.info("Wrote silver table %s.%s -> %s", SILVER_DATABASE, table_name, output_path)


# ----------------------------------------------------------------
# Source: UNAD (CSV CP1252 with institutional preamble)
# ----------------------------------------------------------------
def process_unad() -> None:
    keys = list_s3_objects("unad/", suffix_filter=(".csv",))
    if not keys:
        logger.warning("No UNAD files found in s3://%s/unad/", RAW_BUCKET)
        return

    pdf_parts: List[pd.DataFrame] = []
    for key in keys:
        try:
            body = get_s3_bytes(key)
            text = body.decode("windows-1252", errors="replace")
            lines = text.split("\n")
            skip = 0
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith("UNIVERSIDAD") or stripped.startswith("Estudiantes del periodo"):
                    skip = i + 1
                    continue
                break
            csv_text = "\n".join(lines[skip:])
            pdf = pd.read_csv(
                io.StringIO(csv_text),
                sep=";",
                dtype=str,
                na_values=["", "NULL", "NA"],
            )
            pdf.columns = [normalize_column_name(c) for c in pdf.columns]

            filename = key.split("/")[-1]
            periodo: Optional[str] = None
            if "/periodo=" in key:
                periodo = key.split("/periodo=")[1].split("/")[0]
            if not periodo and "estudiantes_" in filename:
                periodo = filename.replace("estudiantes_", "").replace(".csv", "")
            pdf["_periodo_codigo"] = periodo or "unknown"
            pdf["_source_file"] = filename
            pdf_parts.append(pdf)
        except Exception as exc:
            logger.exception("Failed to parse UNAD file %s: %s", key, exc)

    if not pdf_parts:
        logger.warning("No UNAD files could be parsed")
        return

    full_pdf = pd.concat(pdf_parts, ignore_index=True, sort=False).fillna("")
    df = spark.createDataFrame(full_pdf.astype(str))

    cleaned = df
    if "id" in df.columns:
        cleaned = cleaned.withColumn("id", col("id").cast("long"))
    if "edad" in df.columns:
        cleaned = cleaned.withColumn("edad", col("edad").cast("int"))
    if "estrato_social" in df.columns:
        cleaned = cleaned.withColumn("estrato_social", col("estrato_social").cast("int"))

    text_columns = [
        "sexo",
        "zona_de_residencia",
        "escuela",
        "programa",
        "zona",
        "centro",
        "departamento_residencia",
        "municipio_residencia",
    ]
    for c in text_columns:
        if c in df.columns:
            cleaned = cleaned.withColumn(c, upper(trim(col(c))))

    cleaned = cleaned.withColumn(
        "anio_academico",
        when(col("_periodo_codigo").rlike(r"^17\d{2}$"), 2024)
        .when(col("_periodo_codigo").rlike(r"^203[1-5]$"), 2025)
        .otherwise(None)
        .cast("int"),
    )

    cleaned = add_silver_metadata(cleaned, "unad")
    write_silver_table(
        cleaned,
        "unad_estudiantes",
        partition_cols=["anio_academico", "_periodo_codigo"],
    )


# ----------------------------------------------------------------
# Source: Datos Abiertos Colombia (Socrata JSON)
# ----------------------------------------------------------------
def process_gobierno() -> None:
    keys = list_s3_objects("gobierno/", suffix_filter=(".json",))
    if not keys:
        logger.warning("No Gobierno files found in s3://%s/gobierno/", RAW_BUCKET)
        return

    by_dataset: Dict[str, List[str]] = {}
    for key in keys:
        if "/dataset_id=" in key:
            ds = key.split("/dataset_id=")[1].split("/")[0]
        else:
            base = key.split("/")[-1].replace(".json", "")
            ds = base.replace("dataset_", "")
        by_dataset.setdefault(ds, []).append(key)

    for ds_id, ds_keys in by_dataset.items():
        table_name = GOBIERNO_TABLE_MAP.get(ds_id, f"gobierno_{ds_id.replace('-', '_')}")
        try:
            paths = [f"s3://{RAW_BUCKET}/{k}" for k in ds_keys]
            df = spark.read.option("multiLine", "true").json(paths)
            for c in df.columns:
                df = df.withColumnRenamed(c, normalize_column_name(c))
            df = df.withColumn("_dataset_id", lit(ds_id))
            df = add_silver_metadata(df, "gobierno")
            write_silver_table(df, table_name)
        except Exception as exc:
            logger.exception("Failed to process gobierno dataset %s: %s", ds_id, exc)


# ----------------------------------------------------------------
# Source: SNIES (xlsx + xlsb, variable header row)
# ----------------------------------------------------------------
def process_snies() -> None:
    keys = list_s3_objects("snies/", suffix_filter=(".xlsx", ".xlsb", ".xlsm"))
    if not keys:
        logger.warning("No SNIES files found in s3://%s/snies/", RAW_BUCKET)
        return

    pdf_parts: List[pd.DataFrame] = []
    for key in keys:
        try:
            body = get_s3_bytes(key)
            engine = "pyxlsb" if key.lower().endswith(".xlsb") else "openpyxl"

            preview_buffer = io.BytesIO(body)
            preview = pd.read_excel(
                preview_buffer, engine=engine, header=None, nrows=20, sheet_name=0
            )
            header_row: Optional[int] = None
            for i in range(len(preview)):
                row_values = [str(v).upper() for v in preview.iloc[i].tolist()]
                if any(("CODIGO" in v) or ("CÓDIGO" in v) for v in row_values):
                    header_row = i
                    break
            if header_row is None:
                logger.warning("No header detected in SNIES file %s; skipping", key)
                continue

            full_buffer = io.BytesIO(body)
            pdf = pd.read_excel(
                full_buffer,
                engine=engine,
                header=header_row,
                sheet_name=0,
                dtype=str,
            )
            pdf = pdf.dropna(how="all")
            pdf.columns = [normalize_column_name(c) for c in pdf.columns]
            pdf["_source_file"] = key.split("/")[-1]
            pdf_parts.append(pdf)
        except Exception as exc:
            logger.exception("Failed to parse SNIES file %s: %s", key, exc)

    if not pdf_parts:
        return

    full_pdf = pd.concat(pdf_parts, ignore_index=True, sort=False).fillna("")
    df = spark.createDataFrame(full_pdf.astype(str))
    df = add_silver_metadata(df, "snies")
    write_silver_table(df, "snies_graduados")


# ----------------------------------------------------------------
# Source: SPADIES (UTF-8 BOM, pivot CSV)
# ----------------------------------------------------------------
def process_spadies() -> None:
    keys = list_s3_objects("spadies/", suffix_filter=(".csv",))
    if not keys:
        logger.warning("No SPADIES files found in s3://%s/spadies/", RAW_BUCKET)
        return

    pdf_parts: List[pd.DataFrame] = []
    for key in keys:
        try:
            body = get_s3_bytes(key)
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
            long["valor_str"] = (
                long["valor_str"].astype(str).str.replace("%", "", regex=False).str.replace(",", ".", regex=False)
            )
            long["porcentaje"] = pd.to_numeric(long["valor_str"], errors="coerce")
            long["_source_file"] = key.split("/")[-1]
            pdf_parts.append(long)
        except Exception as exc:
            logger.exception("Failed to parse SPADIES file %s: %s", key, exc)

    if not pdf_parts:
        return

    full_pdf = pd.concat(pdf_parts, ignore_index=True)
    full_pdf["valor_str"] = full_pdf["valor_str"].fillna("")
    full_pdf["porcentaje"] = full_pdf["porcentaje"].fillna(0.0)
    df = spark.createDataFrame(full_pdf)
    df = add_silver_metadata(df, "spadies")
    write_silver_table(df, "spadies_creditos_icetex")


def main() -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}")
    sources = [
        ("unad", process_unad),
        ("gobierno", process_gobierno),
        ("snies", process_snies),
        ("spadies", process_spadies),
    ]
    for label, fn in sources:
        try:
            logger.info("=== Processing source: %s ===", label)
            fn()
        except Exception as exc:
            logger.exception("Source '%s' failed at top-level: %s", label, exc)

    job.commit()


main()

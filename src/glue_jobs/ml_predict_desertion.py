"""
PROYECTO: Data Lake Académico
MÓDULO: ml_predict_desertion.py  (Glue Python Shell, Python 3.9, --library-set=analytics)
DESCRIPCIÓN: Scoring batch de riesgo de deserción. Lee el modelo `latest` del
             bucket de artifacts, puntúa fact_estudiante_semestre y escribe
             gold/ml_predictions_desertion/ en parquet (lo registra el crawler).

Comportamiento defensivo: si aún no existe un modelo entrenado, el job termina
exitoso sin escribir nada (el pipeline ETL nunca falla por el componente ML).
Los agregados del dashboard NO se generan aquí: son responsabilidad exclusiva
del job publish-dashboard (un solo dueño, nada de CSV/JSON en el bucket gold).
"""

import io
import json
import logging
import sys
from datetime import datetime, timezone

import boto3
import joblib
import numpy as np
import pandas as pd
import sklearn
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger("ml_predict_desertion")

args = getResolvedOptions(sys.argv, ["GOLD_BUCKET", "ARTIFACTS_BUCKET", "MODEL_PREFIX", "PREDICTIONS_PREFIX"])
GOLD_BUCKET = args["GOLD_BUCKET"]
ARTIFACTS_BUCKET = args["ARTIFACTS_BUCKET"]
MODEL_PREFIX = args["MODEL_PREFIX"].strip("/")
PREDICTIONS_PREFIX = args["PREDICTIONS_PREFIX"].strip("/")

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
s3 = boto3.client("s3")

UNKNOWN_CATEGORY = "DESCONOCIDO"
OTHER_CATEGORY = "OTRO"


def get_artifact(key: str) -> bytes:
    return s3.get_object(Bucket=ARTIFACTS_BUCKET, Key=key)["Body"].read()


def build_features(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Réplica exacta de build_features del training (misma transformación)."""
    out = df.copy()
    for col in ["edad", "estrato_social", "nivel_conectividad", "n_periodos_en_semestre",
                "semestres_cursados_acum", "gap_desde_semestre_anterior", "es_primer_semestre"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["log_poblacion_sisben_mpio"] = np.log1p(pd.to_numeric(out["poblacion_sisben_mpio"], errors="coerce"))
    out["log_total_accesos_res"] = np.log1p(pd.to_numeric(out["total_accesos_res"], errors="coerce"))
    # Mismo forzado a numpy float64 que en el training (evita pd.NA -> sklearn rompe).
    for col in schema["numeric_features"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    for col in schema["categorical_features"]:
        out[col] = out[col].fillna(UNKNOWN_CATEGORY).astype(str).str.strip().str.upper()
        out[col] = out[col].where(out[col].isin(schema["categories"][col]), OTHER_CATEGORY)
    return out


def main() -> None:
    # 1. Modelo latest (salida limpia si no existe)
    try:
        schema = json.loads(get_artifact(f"{MODEL_PREFIX}/latest/feature_schema.json"))
        metrics = json.loads(get_artifact(f"{MODEL_PREFIX}/latest/metrics.json"))
        model_bytes = get_artifact(f"{MODEL_PREFIX}/latest/model.joblib")
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            logger.warning("No existe modelo en latest/: el job termina sin puntuar (primera corrida).")
            return
        raise

    # 2. Guard de versión: el joblib solo es válido con el mismo sklearn del training
    trained_with = schema.get("sklearn_version")
    if trained_with != sklearn.__version__:
        raise RuntimeError(
            f"Version mismatch: modelo entrenado con sklearn=={trained_with}, "
            f"runtime tiene sklearn=={sklearn.__version__}. Reentrenar o alinear versiones.")

    model = joblib.load(io.BytesIO(model_bytes))
    model_version = schema["run_id"]
    threshold = float(metrics["threshold"]["threshold"])
    logger.info(f"Modelo {model_version} cargado; threshold={threshold:.4f}")

    # 3. Dataset completo (se puntúan TODOS los semestres para análisis retrospectivo)
    import awswrangler as wr
    # dataset=True: fact_estudiante_semestre esta particionado por anio (Hive), asi
    # awswrangler reconstruye la columna 'anio' desde el path (requerida abajo para
    # el output y partition_cols). Sin esto, df['anio'] lanzaria KeyError.
    df = wr.s3.read_parquet(path=f"s3://{GOLD_BUCKET}/fact_estudiante_semestre/", dataset=True)
    logger.info(f"fact_estudiante_semestre: {len(df)} filas")

    feats = build_features(df, schema)
    feature_cols = schema["numeric_features"] + schema["categorical_features"]
    scores = model.predict_proba(feats[feature_cols])[:, 1]

    # 4. Bandas de riesgo con el threshold congelado del training
    banda = np.where(scores >= threshold, "alto",
             np.where(scores >= 0.5 * threshold, "medio", "bajo"))

    out = pd.DataFrame({
        "id": df["id"].astype(str),
        "anio": df["anio"],
        "semestre_academico": df["semestre_academico"],
        "semestre_orden": df["semestre_orden"],
        "score_riesgo": scores.astype(float),
        "banda_riesgo": banda,
        "threshold": threshold,
        "model_version": model_version,
        "_predict_run_id": RUN_ID,
        "_predicted_at": datetime.now(timezone.utc).isoformat(),
    })

    # 5. Escritura parquet a gold (overwrite completo del prefijo; SSE-KMS por default del bucket)
    path = f"s3://{GOLD_BUCKET}/{PREDICTIONS_PREFIX}/"
    wr.s3.to_parquet(df=out, path=path, dataset=True, mode="overwrite",
                     partition_cols=["anio"], index=False)
    resumen = out.groupby("banda_riesgo")["id"].count().to_dict()
    logger.info(f"Predicciones escritas en {path}: {len(out)} filas; bandas={resumen}")


if __name__ == "__main__":
    main()

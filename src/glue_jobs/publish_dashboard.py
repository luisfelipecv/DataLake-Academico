"""
PROYECTO: Data Lake Académico
MÓDULO: publish_dashboard.py  (Glue Python Shell, Python 3.9, --library-set=analytics)
DESCRIPCIÓN: Único dueño de los agregados del dashboard. Lee las tablas Gold
             (fact_estudiante_semestre, fact_estudiante_periodo, predicciones ML
             y métricas del modelo), aplica las reglas de disociación de la
             Ley 1581 y publica CSV/JSON al bucket del sitio web
             ({project}-web-{account}), invalidando después /data/* en CloudFront.

Reglas de publicación (Ley 1581 / dato disociado):
  - SOLO agregados: jamás ids ni filas a nivel estudiante.
  - Supresión k<MIN_CELL: celdas pequeñas se colapsan en "OTROS".
  - Supresión complementaria: el grupo OTROS debe tener >=2 celdas hijas y
    >=MIN_CELL estudiantes (si no, absorbe la siguiente celda más pequeña).
  - Homogeneidad: celdas con deserción 0% o 100% no publican tasa (baja_confianza).
  - Tasas solo con evaluables >= MIN_RATE_N; si no, baja_confianza=true.
  - Una dimensión demográfica por archivo (sin cruces finos re-identificables).

Comportamiento defensivo: si el bucket web no existe (frontend no desplegado),
el job termina exitoso sin publicar nada.
"""

import json
import logging
import sys
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger("publish_dashboard")

args = getResolvedOptions(sys.argv, ["GOLD_BUCKET", "ARTIFACTS_BUCKET", "PROJECT_NAME",
                                     "MODEL_PREFIX", "PREDICTIONS_PREFIX", "MIN_CELL"])
GOLD_BUCKET = args["GOLD_BUCKET"]
ARTIFACTS_BUCKET = args["ARTIFACTS_BUCKET"]
PROJECT_NAME = args["PROJECT_NAME"]
MODEL_PREFIX = args["MODEL_PREFIX"].strip("/")
PREDICTIONS_PREFIX = args["PREDICTIONS_PREFIX"].strip("/")
MIN_CELL = int(args["MIN_CELL"])
MIN_RATE_N = 30

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
s3 = boto3.client("s3")
OTHER_LABEL = "OTROS"


def resolve_web_bucket() -> str:
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"{PROJECT_NAME}-web-{account_id}"


def bucket_exists(bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def rate_columns(g: pd.DataFrame) -> pd.DataFrame:
    """Calcula tasa de deserción con las reglas de homogeneidad y confianza."""
    g = g.copy()
    g["tasa_desercion_pct"] = (g["desertores"] / g["evaluables"] * 100).round(2)
    homogenea = (g["desertores"] == 0) | (g["desertores"] == g["evaluables"])
    poca_n = g["evaluables"] < MIN_RATE_N
    g["baja_confianza"] = (homogenea | poca_n)
    g.loc[homogenea, "tasa_desercion_pct"] = np.nan
    return g


def aggregate_with_suppression(df: pd.DataFrame, dims: list, parent: str = None) -> pd.DataFrame:
    """Agrega evaluables/desertores por dims aplicando supresión k<MIN_CELL con
    colapso en OTROS y supresión complementaria dentro de cada padre."""
    g = df.groupby(dims, dropna=False).agg(
        matriculados=("id", "nunique"),
        evaluables=("desertion_t1", lambda s: int(s.notna().sum())),
        desertores=("desertion_t1", lambda s: int(s.fillna(0).sum())),
    ).reset_index()

    child = dims[-1]
    parent_cols = [c for c in dims if c != child] if parent else []

    def collapse(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("matriculados")
        small = group["matriculados"] < MIN_CELL
        keep = group[~small].copy()
        others = group[small]
        # Supresión complementaria: OTROS necesita >=2 hijas y >=MIN_CELL personas
        while len(others) > 0 and (len(others) < 2 or others["matriculados"].sum() < MIN_CELL):
            if keep.empty:
                break
            move = keep.iloc[[0]]
            keep = keep.iloc[1:]
            others = pd.concat([others, move])
        if len(others) > 0:
            row = {c: others.iloc[0][c] for c in parent_cols}
            row[child] = OTHER_LABEL
            row["matriculados"] = int(others["matriculados"].sum())
            row["evaluables"] = int(others["evaluables"].sum())
            row["desertores"] = int(others["desertores"].sum())
            keep = pd.concat([keep, pd.DataFrame([row])], ignore_index=True)
        return keep

    if parent_cols:
        g = g.groupby(parent_cols, group_keys=False).apply(collapse).reset_index(drop=True)
    else:
        g = collapse(g).reset_index(drop=True)
    return rate_columns(g)


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8").encode("utf-8")


def main() -> None:
    web_bucket = resolve_web_bucket()
    if not bucket_exists(web_bucket):
        logger.warning(f"Bucket {web_bucket} no existe (frontend no desplegado). Job finaliza sin publicar.")
        return

    import awswrangler as wr
    sem = wr.s3.read_parquet(path=f"s3://{GOLD_BUCKET}/fact_estudiante_semestre/")
    logger.info(f"fact_estudiante_semestre: {len(sem)} filas")
    sem["desertion_t1"] = pd.to_numeric(sem["desertion_t1"], errors="coerce")
    sem["estrato_social"] = pd.to_numeric(sem["estrato_social"], errors="coerce")

    files: dict = {}

    # --- 1. Matrícula y deserción por semestre (tendencia) ---
    por_sem = sem.groupby(["semestre_academico", "semestre_orden"]).agg(
        matriculados=("id", "nunique"),
        evaluables=("desertion_t1", lambda s: int(s.notna().sum())),
        desertores=("desertion_t1", lambda s: int(s.fillna(0).sum())),
    ).reset_index().sort_values("semestre_orden")
    por_sem = rate_columns(por_sem)
    files["desercion_por_semestre.csv"] = por_sem

    # --- 2. Por escuela / por programa (con supresión complementaria) ---
    files["desercion_por_escuela.csv"] = aggregate_with_suppression(sem, ["escuela"])
    files["desercion_por_programa.csv"] = aggregate_with_suppression(
        sem, ["escuela", "programa"], parent="escuela")

    # --- 3. Demografía (una dimensión por bloque, formato tidy) ---
    demo_frames = []
    for dim in ["sexo", "zona_de_residencia", "estrato_social", "nivel_conectividad"]:
        d = sem.copy()
        d[dim] = d[dim].astype(str).replace({"nan": "SIN DATO", "<NA>": "SIN DATO"})
        agg = aggregate_with_suppression(d, [dim])
        agg = agg.rename(columns={dim: "categoria"})
        agg.insert(0, "dimension", dim)
        demo_frames.append(agg)
    files["desercion_demografia.csv"] = pd.concat(demo_frames, ignore_index=True)

    # --- 4. Mapa por departamento (tasa + contexto socioeconómico) ---
    depto = sem.groupby("departamento_residencia").agg(
        matriculados=("id", "nunique"),
        evaluables=("desertion_t1", lambda s: int(s.notna().sum())),
        desertores=("desertion_t1", lambda s: int(s.fillna(0).sum())),
        promedio_ipm_mpio=("promedio_ipm_mpio", "mean"),
        tasa_informalidad_mpio=("tasa_informalidad_mpio", "mean"),
        cobertura_divipola=("codigo_divipola", lambda s: float(s.notna().mean())),
    ).reset_index()
    depto = depto[depto["matriculados"] >= MIN_CELL]
    depto = rate_columns(depto)
    # Contexto municipal solo donde el cruce DIVIPOLA cubre >=50% del depto
    depto.loc[depto["cobertura_divipola"] < 0.5,
              ["promedio_ipm_mpio", "tasa_informalidad_mpio"]] = np.nan
    depto["promedio_ipm_mpio"] = depto["promedio_ipm_mpio"].round(4)
    depto["tasa_informalidad_mpio"] = depto["tasa_informalidad_mpio"].round(4)
    files["mapa_departamento.csv"] = depto.drop(columns=["cobertura_divipola"])

    # --- 5. Riesgo predicho (si existen predicciones) ---
    model_metrics = None
    try:
        preds = wr.s3.read_parquet(path=f"s3://{GOLD_BUCKET}/{PREDICTIONS_PREFIX}/")
        raw_metrics = json.loads(s3.get_object(
            Bucket=ARTIFACTS_BUCKET, Key=f"{MODEL_PREFIX}/latest/metrics.json")["Body"].read())
        vigente = int(preds["semestre_orden"].max())
        pv = preds[preds["semestre_orden"] == vigente].merge(
            sem[sem["semestre_orden"] == vigente][["id", "escuela", "programa"]],
            on="id", how="left")
        riesgo = pv.groupby(["escuela", "programa"]).agg(
            n_estudiantes=("id", "nunique"),
            n_riesgo_alto=("banda_riesgo", lambda s: int((s == "alto").sum())),
            prob_promedio=("score_riesgo", "mean"),
        ).reset_index()
        riesgo = riesgo[riesgo["n_estudiantes"] >= MIN_CELL].copy()
        # Celdas con 100% o 0% de riesgo alto no se detallan (homogeneidad)
        homog = (riesgo["n_riesgo_alto"] == 0) | (riesgo["n_riesgo_alto"] == riesgo["n_estudiantes"])
        riesgo["pct_riesgo_alto"] = (riesgo["n_riesgo_alto"] / riesgo["n_estudiantes"] * 100).round(2)
        riesgo.loc[homog, "pct_riesgo_alto"] = np.nan
        riesgo["baja_confianza"] = homog
        riesgo["prob_promedio"] = riesgo["prob_promedio"].round(4)
        riesgo["semestre_academico"] = preds.loc[preds["semestre_orden"] == vigente,
                                                 "semestre_academico"].iloc[0]
        files["riesgo_por_programa.csv"] = riesgo

        # Métricas publicables del modelo (sin internals)
        model_metrics = {
            "modelo": raw_metrics["model"],
            "version": raw_metrics["run_id"],
            "fecha_entrenamiento": raw_metrics["trained_at"],
            "validacion": {"holdout": 0.2, "kfold": 5, "agrupado_por_estudiante": True},
            "umbral": raw_metrics["threshold"],
            "test": raw_metrics["test"],
            "cv": {k: raw_metrics["validation"]["kfold"][k]
                   for k in ["f1_mean_default", "f1_std_default", "pr_auc_mean", "pr_auc_std"]},
            "temporal": raw_metrics.get("temporal"),
            "baseline_logreg_test": raw_metrics.get("baseline_logreg_test"),
            "meta_80_80": raw_metrics.get("target_goal"),
            "exclusiones": raw_metrics.get("dataset", {}),
            "curva_pr_test": raw_metrics.get("pr_curve_test"),
        }
        files["metricas_modelo.json"] = model_metrics
        files["importancia_variables.csv"] = pd.DataFrame(
            raw_metrics["permutation_importance"]).assign(
            ranking=lambda d: range(1, len(d) + 1))
    except ClientError:
        logger.warning("Sin predicciones/métricas de modelo aún: se publican solo los descriptivos.")
    except Exception as exc:  # noqa: BLE001 - el dashboard descriptivo no debe caerse por el ML
        logger.warning(f"No se pudo publicar el bloque predictivo: {exc}")

    # --- 6. KPIs ---
    evaluables = por_sem["evaluables"].sum()
    desertores = por_sem["desertores"].sum()
    tasa_global = desertores / evaluables * 100 if evaluables else np.nan
    brecha_estrato = _brecha(files["desercion_demografia.csv"], "estrato_social",
                             bajo=["0.0", "0", "1.0", "1"], alto=["3.0", "3", "4.0", "4", "5.0", "5"])
    brecha_conect = _brecha(files["desercion_demografia.csv"], "nivel_conectividad",
                            bajo=["0.0", "0", "1.0", "1"], alto=["3.0", "3"])
    kpis = [
        {"id": "tasa_desercion", "etiqueta": "Tasa de deserción intersemestral",
         "valor": round(tasa_global, 2), "unidad": "%",
         "definicion": "Estudiantes sin matrícula en el semestre siguiente / evaluables (excluye el último semestre, censurado)"},
        {"id": "tasa_retencion", "etiqueta": "Tasa de retención",
         "valor": round(100 - tasa_global, 2), "unidad": "%",
         "definicion": "100 - tasa de deserción intersemestral"},
        {"id": "matricula_ultimo_semestre", "etiqueta": "Matriculados último semestre",
         "valor": int(por_sem.iloc[-1]["matriculados"]), "unidad": "",
         "definicion": f"Estudiantes únicos en {por_sem.iloc[-1]['semestre_academico']}"},
        {"id": "brecha_estrato", "etiqueta": "Brecha por estrato (0-1 vs 3+)",
         "valor": brecha_estrato, "unidad": "p.p.",
         "definicion": "Diferencia de tasa de deserción entre estratos 0-1 y estratos 3 o más"},
        {"id": "brecha_conectividad", "etiqueta": "Brecha por conectividad (baja vs alta)",
         "valor": brecha_conect, "unidad": "p.p.",
         "definicion": "Diferencia de tasa de deserción entre municipios de conectividad baja/nula y alta"},
    ]
    if "riesgo_por_programa.csv" in files:
        r = files["riesgo_por_programa.csv"]
        kpis.append({"id": "riesgo_alto", "etiqueta": "Estudiantes en riesgo alto (semestre vigente)",
                     "valor": int(r["n_riesgo_alto"].sum()), "unidad": "",
                     "definicion": "Score de riesgo >= umbral del modelo en el último semestre puntuado"})
    if model_metrics:
        kpis.append({"id": "f1_modelo", "etiqueta": "F1 del modelo (holdout)",
                     "valor": round(model_metrics["test"]["f1"], 3), "unidad": "",
                     "definicion": "F1 de la clase deserción en el 20% de estudiantes nunca vistos"})
    files["kpis.json"] = kpis

    files["manifest.json"] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "publish_run_id": RUN_ID,
        "min_cell_threshold": MIN_CELL,
        "min_rate_n": MIN_RATE_N,
        "model_version": (model_metrics or {}).get("version"),
        "files": sorted([f"data/{k}" for k in files if k != "manifest.json"] + ["data/manifest.json"]),
        "nota_ley_1581": ("Todos los datos publicados son agregados disociados: celdas con menos de "
                          f"{MIN_CELL} estudiantes suprimidas o colapsadas; tasas 0%/100% no publicadas."),
    }

    # --- 7. Publicación al bucket web ---
    for name, content in files.items():
        if isinstance(content, pd.DataFrame):
            body, ctype = to_csv_bytes(content), "text/csv; charset=utf-8"
        else:
            body = json.dumps(content, ensure_ascii=False, indent=2, default=str).encode("utf-8")
            ctype = "application/json"
        s3.put_object(Bucket=web_bucket, Key=f"data/{name}", Body=body, ContentType=ctype)
        logger.info(f"Publicado s3://{web_bucket}/data/{name} ({len(body)} bytes)")

    # --- 8. Invalidación de CloudFront (lookup por dominio del origin) ---
    try:
        cf = boto3.client("cloudfront")
        target = None
        for dist in cf.list_distributions().get("DistributionList", {}).get("Items", []):
            for origin in dist["Origins"]["Items"]:
                if web_bucket in origin["DomainName"]:
                    target = dist["Id"]
        if target:
            cf.create_invalidation(DistributionId=target, InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/data/*"]},
                "CallerReference": f"publish-{RUN_ID}"})
            logger.info(f"Invalidación /data/* creada en distribución {target}")
        else:
            logger.warning("No se encontró distribución CloudFront para el bucket web; sin invalidación.")
    except ClientError as exc:
        logger.warning(f"Invalidación omitida: {exc}")


def _brecha(demo: pd.DataFrame, dimension: str, bajo: list, alto: list):
    d = demo[demo["dimension"] == dimension]
    t_bajo = d[d["categoria"].isin(bajo)]
    t_alto = d[d["categoria"].isin(alto)]
    if t_bajo["evaluables"].sum() == 0 or t_alto["evaluables"].sum() == 0:
        return None
    r_bajo = t_bajo["desertores"].sum() / t_bajo["evaluables"].sum() * 100
    r_alto = t_alto["desertores"].sum() / t_alto["evaluables"].sum() * 100
    return round(r_bajo - r_alto, 2)


if __name__ == "__main__":
    main()

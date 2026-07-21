"""
PROYECTO: Data Lake Académico
MÓDULO: ml_train_desertion.py  (Glue Python Shell, Python 3.9, --library-set=analytics)
DESCRIPCIÓN: Entrena el modelo de deserción estudiantil sobre
             gold/fact_estudiante_semestre y publica los artefactos en el
             bucket de artifacts (model.joblib, metrics.json, feature_schema.json).

Protocolo metodológico (definido en el plan aprobado):
  1. Holdout PRIMERO: GroupShuffleSplit 80/20 agrupado por id de estudiante.
  2. StratifiedGroupKFold k=5 DENTRO del 80% (tuning y OOF solo con train).
  3. Threshold: maximizar recall sujeto a precision >= MIN_PRECISION sobre OOF;
     si ningún threshold alcanza esa precision, se usa el de máximo F1 y se
     registra el hecho en metrics.json (transparencia, no goalpost-moving).
  4. Holdout evaluado UNA sola vez con el threshold congelado.
  5. Validación temporal complementaria: train semestres 1..k-1, test semestre k.
  6. Baseline LogisticRegression para evidenciar el lift del modelo.
Leakage: quedan EXCLUIDAS las columnas de historia completa (total_periodos_*,
tasa_permanencia, etc.) que solo existen en la tabla de BI; esta tabla de
entrenamiento solo trae historia as-of-t.
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
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    confusion_matrix, f1_score, precision_recall_curve, precision_score,
    recall_score, roc_auc_score, average_precision_score
)
from sklearn.model_selection import GroupShuffleSplit, StratifiedGroupKFold
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler
from sklearn.utils.class_weight import compute_sample_weight

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger("ml_train_desertion")

args = getResolvedOptions(sys.argv, ["GOLD_BUCKET", "ARTIFACTS_BUCKET", "MODEL_PREFIX", "MIN_PRECISION"])
GOLD_BUCKET = args["GOLD_BUCKET"]
ARTIFACTS_BUCKET = args["ARTIFACTS_BUCKET"]
MODEL_PREFIX = args["MODEL_PREFIX"].strip("/")
MIN_PRECISION = float(args["MIN_PRECISION"])

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
RANDOM_STATE = 42
MAX_CATEGORIES = 200  # tope por feature categórica (límite 255 de HistGB)

s3 = boto3.client("s3")

# Contrato de features (única fuente de verdad; el predict lo lee del schema publicado)
NUMERIC_FEATURES = [
    "edad", "estrato_social", "n_periodos_en_semestre",
    "semestres_cursados_acum", "gap_desde_semestre_anterior", "es_primer_semestre",
    "tasa_informalidad_mpio", "tasa_hacinamiento_mpio", "promedio_ipm_mpio",
    "log_poblacion_sisben_mpio", "log_total_accesos_res", "nivel_conectividad",
]
CATEGORICAL_FEATURES = [
    "sexo", "zona_de_residencia", "escuela", "programa", "zona", "centro",
    "departamento_residencia", "cohorte_tipo",
]
TARGET = "desertion_t1"
UNKNOWN_CATEGORY = "DESCONOCIDO"
OTHER_CATEGORY = "OTRO"


def read_dataset() -> pd.DataFrame:
    """Lee fact_estudiante_semestre directamente del parquet de Gold (sin Athena)."""
    import awswrangler as wr
    cols = ["id", "semestre_orden", "semestre_academico", TARGET,
            "edad", "estrato_social", "n_periodos_en_semestre",
            "semestres_cursados_acum", "gap_desde_semestre_anterior", "es_primer_semestre",
            "tasa_informalidad_mpio", "tasa_hacinamiento_mpio", "promedio_ipm_mpio",
            "poblacion_sisben_mpio", "total_accesos_res", "nivel_conectividad"] + CATEGORICAL_FEATURES
    df = wr.s3.read_parquet(path=f"s3://{GOLD_BUCKET}/fact_estudiante_semestre/", columns=None)
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Columnas faltantes en fact_estudiante_semestre: {missing}")
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Transformaciones deterministas previas al pipeline sklearn.
    IDÉNTICA a la versión del notebook 03_modelo_desercion.ipynb."""
    out = df.copy()
    for col in ["edad", "estrato_social", "nivel_conectividad", "n_periodos_en_semestre",
                "semestres_cursados_acum", "gap_desde_semestre_anterior", "es_primer_semestre"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["log_poblacion_sisben_mpio"] = np.log1p(pd.to_numeric(out["poblacion_sisben_mpio"], errors="coerce"))
    out["log_total_accesos_res"] = np.log1p(pd.to_numeric(out["total_accesos_res"], errors="coerce"))
    # awswrangler puede devolver dtypes nullable (Int64/Float64 con pd.NA) al leer el
    # parquet de gold; sklearn rompe con np.asarray(dtype=float) sobre pd.NA
    # ("float() argument ... not 'NAType'"). Forzamos numpy float64 en TODAS las
    # features numericas (pd.NA -> np.nan; HistGradientBoosting maneja np.nan nativo).
    for col in NUMERIC_FEATURES:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    for col in CATEGORICAL_FEATURES:
        out[col] = out[col].fillna(UNKNOWN_CATEGORY).astype(str).str.strip().str.upper()
    return out


def cap_categories(train_df: pd.DataFrame) -> dict:
    """Congela por feature las categorías admitidas (top MAX_CATEGORIES del train);
    el resto se colapsa en OTRO. Devuelve el mapa para persistir en el schema."""
    cats = {}
    for col in CATEGORICAL_FEATURES:
        top = train_df[col].value_counts().index[:MAX_CATEGORIES].tolist()
        cats[col] = sorted(set(top) | {OTHER_CATEGORY, UNKNOWN_CATEGORY})
    return cats


def apply_category_map(df: pd.DataFrame, cats: dict) -> pd.DataFrame:
    out = df.copy()
    for col, allowed in cats.items():
        out[col] = out[col].where(out[col].isin(allowed), OTHER_CATEGORY)
    return out


def make_model(categories: dict) -> Pipeline:
    """HistGradientBoosting con soporte nativo de categóricas (sklearn 1.0.2).
    El OrdinalEncoder usa categorías CONGELADAS y un unknown_value NO negativo
    (= len(categorías)), requisito de categorical_features en 1.0.x."""
    cat_lists = [categories[c] for c in CATEGORICAL_FEATURES]
    n_cats = max(len(c) for c in cat_lists) + 1
    encoder = ColumnTransformer([
        ("num", "passthrough", NUMERIC_FEATURES),
        ("cat", OrdinalEncoder(categories=cat_lists,
                               handle_unknown="use_encoded_value",
                               unknown_value=max(len(c) for c in cat_lists)),
         CATEGORICAL_FEATURES),
    ])
    cat_mask = [False] * len(NUMERIC_FEATURES) + [True] * len(CATEGORICAL_FEATURES)
    clf = HistGradientBoostingClassifier(
        categorical_features=cat_mask,
        max_bins=min(255, max(n_cats + 1, 255)),
        random_state=RANDOM_STATE,
    )
    return Pipeline([("encoder", encoder), ("clf", clf)])


def make_baseline() -> Pipeline:
    # LogisticRegression NO tolera NaN: imputamos por mediana antes de escalar.
    # (El modelo principal HistGB maneja NaN nativo y no requiere imputacion.)
    encoder = ColumnTransformer([
        ("num", Pipeline([("impute", SimpleImputer(strategy="median")),
                          ("scale", StandardScaler())]), NUMERIC_FEATURES),
        # sin max_categories: ese kwarg es de sklearn>=1.1 y el runtime Glue analytics
        # trae sklearn 1.0.2 (max_categories=None equivale al default, no limitar).
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
    ])
    return Pipeline([("encoder", encoder),
                     ("clf", LogisticRegression(max_iter=1000, class_weight="balanced",
                                                random_state=RANDOM_STATE))])


def fit_with_weights(pipeline: Pipeline, X: pd.DataFrame, y: np.ndarray) -> Pipeline:
    sw = compute_sample_weight("balanced", y)
    pipeline.fit(X, y, clf__sample_weight=sw)
    return pipeline


def tune_threshold(y_true: np.ndarray, y_score: np.ndarray) -> dict:
    """Máximo recall sujeto a precision >= MIN_PRECISION sobre OOF.
    Fallback documentado: threshold de máximo F1."""
    precision, recall, thresholds = precision_recall_curve(y_true, y_score)
    # precision/recall tienen len = len(thresholds)+1; alineamos descartando el último punto
    precision, recall = precision[:-1], recall[:-1]
    feasible = precision >= MIN_PRECISION
    if feasible.any():
        idx = np.argmax(np.where(feasible, recall, -1.0))
        return {"threshold": float(thresholds[idx]), "criterion": f"max recall s.t. precision>={MIN_PRECISION}",
                "oof_precision": float(precision[idx]), "oof_recall": float(recall[idx])}
    f1 = 2 * precision * recall / np.clip(precision + recall, 1e-9, None)
    idx = int(np.argmax(f1))
    return {"threshold": float(thresholds[idx]),
            "criterion": f"max F1 (NINGUN threshold alcanzo precision>={MIN_PRECISION} en OOF)",
            "oof_precision": float(precision[idx]), "oof_recall": float(recall[idx])}


def evaluate(y_true: np.ndarray, y_score: np.ndarray, threshold: float) -> dict:
    y_pred = (y_score >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return {
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, y_score)),
        "pr_auc": float(average_precision_score(y_true, y_score)),
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
        "n": int(len(y_true)), "prevalence": float(np.mean(y_true)),
    }


def put_artifact(key: str, body: bytes) -> None:
    s3.put_object(Bucket=ARTIFACTS_BUCKET, Key=key, Body=body, ServerSideEncryption="AES256")


def _json_default(o):
    """Coacciona escalares/arrays numpy a tipos nativos para json.dumps
    (p.ej. train_semestres es lista de numpy.int32, no serializable directo)."""
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return float(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    raise TypeError(f"No serializable: {type(o).__name__}")


def main() -> None:
    logger.info(f"Inicio de entrenamiento. run_id={RUN_ID}, sklearn={sklearn.__version__}")
    raw = read_dataset()
    logger.info(f"fact_estudiante_semestre: {len(raw)} filas")

    data = build_features(raw)
    labeled = data[data[TARGET].notna()].copy()
    labeled[TARGET] = labeled[TARGET].astype(int)
    logger.info(f"Filas etiquetadas (sin censura): {len(labeled)}; prevalencia={labeled[TARGET].mean():.3f}")

    # 1. Holdout agrupado por estudiante, ANTES de cualquier tuning
    splitter = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=RANDOM_STATE)
    train_idx, test_idx = next(splitter.split(labeled, labeled[TARGET], groups=labeled["id"]))
    train_df, test_df = labeled.iloc[train_idx], labeled.iloc[test_idx]

    cats = cap_categories(train_df)
    train_df = apply_category_map(train_df, cats)
    test_df = apply_category_map(test_df, cats)
    features = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    X_train, y_train = train_df[features], train_df[TARGET].values
    X_test, y_test = test_df[features], test_df[TARGET].values

    # 2. K-fold agrupado + OOF dentro del 80%
    oof_score = np.full(len(train_df), np.nan)
    fold_metrics = []
    cv = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)
    for k, (fit_idx, val_idx) in enumerate(cv.split(X_train, y_train, groups=train_df["id"])):
        model_k = fit_with_weights(make_model(cats), X_train.iloc[fit_idx], y_train[fit_idx])
        scores = model_k.predict_proba(X_train.iloc[val_idx])[:, 1]
        oof_score[val_idx] = scores
        fold_metrics.append({
            "fold": k, "n_val": int(len(val_idx)),
            "prevalence_val": float(np.mean(y_train[val_idx])),
            "f1_default": float(f1_score(y_train[val_idx], (scores >= 0.5).astype(int), zero_division=0)),
            "pr_auc": float(average_precision_score(y_train[val_idx], scores)),
        })
        logger.info(f"Fold {k}: {fold_metrics[-1]}")

    # 3. Threshold congelado sobre OOF
    thr = tune_threshold(y_train, oof_score)
    logger.info(f"Threshold elegido: {thr}")
    cv_summary = {
        "k": 5,
        "f1_mean_default": float(np.mean([m["f1_default"] for m in fold_metrics])),
        "f1_std_default": float(np.std([m["f1_default"] for m in fold_metrics])),
        "pr_auc_mean": float(np.mean([m["pr_auc"] for m in fold_metrics])),
        "pr_auc_std": float(np.std([m["pr_auc"] for m in fold_metrics])),
        "oof": evaluate(y_train, oof_score, thr["threshold"]),
        "folds": fold_metrics,
    }

    # 4. Modelo final sobre TODO el 80% y evaluación única del holdout
    model = fit_with_weights(make_model(cats), X_train, y_train)
    test_scores = model.predict_proba(X_test)[:, 1]
    holdout = evaluate(y_test, test_scores, thr["threshold"])
    logger.info(f"Holdout 20%: {holdout}")

    # 5. Validación temporal: train en semestres tempranos, test en el último etiquetado
    labeled_orders = sorted(labeled["semestre_orden"].unique())
    t_train_orders, t_test_order = labeled_orders[:-1], labeled_orders[-1]
    tr = apply_category_map(labeled[labeled["semestre_orden"].isin(t_train_orders)], cats)
    te = apply_category_map(labeled[labeled["semestre_orden"] == t_test_order], cats)
    temporal = None
    if len(tr) and len(te):
        model_t = fit_with_weights(make_model(cats), tr[features], tr[TARGET].values)
        temporal = evaluate(te[TARGET].values, model_t.predict_proba(te[features])[:, 1], thr["threshold"])
        temporal.update({"train_semestres": t_train_orders, "test_semestre": int(t_test_order)})
        logger.info(f"Validacion temporal: {temporal}")

    # 6. Baseline LogisticRegression (mismo protocolo de holdout)
    baseline = make_baseline()
    baseline.fit(X_train, y_train)
    baseline_holdout = evaluate(y_test, baseline.predict_proba(X_test)[:, 1], thr["threshold"])
    logger.info(f"Baseline holdout: {baseline_holdout}")

    # 7. Importancia de variables por permutación (sobre el holdout, agregada)
    perm = permutation_importance(model, X_test, y_test, n_repeats=5,
                                  random_state=RANDOM_STATE, scoring="average_precision")
    importance = sorted(
        [{"feature": f, "importance_mean": float(m), "importance_std": float(s)}
         for f, m, s in zip(features, perm.importances_mean, perm.importances_std)],
        key=lambda d: -d["importance_mean"])

    # 8. Curva PR del holdout (muestreada para el dashboard)
    p_curve, r_curve, t_curve = precision_recall_curve(y_test, test_scores)
    step = max(1, len(p_curve) // 200)
    pr_curve = [{"precision": float(p), "recall": float(r)}
                for p, r in zip(p_curve[::step], r_curve[::step])]

    metrics = {
        "run_id": RUN_ID,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "model": "HistGradientBoostingClassifier",
        "library_versions": {"python": sys.version.split()[0], "sklearn": sklearn.__version__,
                             "pandas": pd.__version__, "numpy": np.__version__},
        "dataset": {"rows_total": int(len(raw)), "rows_labeled": int(len(labeled)),
                    "prevalence": float(labeled[TARGET].mean()),
                    "n_students": int(labeled["id"].nunique()),
                    "semestres": {str(k): int(v) for k, v in
                                  labeled["semestre_orden"].value_counts().sort_index().items()}},
        "validation": {"holdout_fraction": 0.2, "grouped_by": "id", "kfold": cv_summary},
        "threshold": thr,
        "test": holdout,
        "temporal": temporal,
        "baseline_logreg_test": baseline_holdout,
        "permutation_importance": importance,
        "pr_curve_test": pr_curve,
        "target_goal": {"f1": 0.80, "precision": MIN_PRECISION,
                        "goal_met": bool(holdout["f1"] >= 0.80 and holdout["precision"] >= MIN_PRECISION)},
    }

    schema = {
        "run_id": RUN_ID,
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "categories": cats,
        "target": TARGET,
        "excluded_leakage": ["total_periodos_matriculados", "tasa_permanencia",
                             "primer_periodo", "ultimo_periodo", "next_semestre_orden",
                             "semestre_orden", "anio", "municipio_residencia"],
        "unknown_category": UNKNOWN_CATEGORY, "other_category": OTHER_CATEGORY,
        "sklearn_version": sklearn.__version__,
    }

    buf = io.BytesIO()
    joblib.dump(model, buf, compress=3)
    for prefix in (f"{MODEL_PREFIX}/{RUN_ID}", f"{MODEL_PREFIX}/latest"):
        put_artifact(f"{prefix}/model.joblib", buf.getvalue())
        put_artifact(f"{prefix}/metrics.json", json.dumps(metrics, ensure_ascii=False, indent=2, default=_json_default).encode())
        put_artifact(f"{prefix}/feature_schema.json", json.dumps(schema, ensure_ascii=False, indent=2, default=_json_default).encode())
    logger.info(f"Artefactos publicados en s3://{ARTIFACTS_BUCKET}/{MODEL_PREFIX}/{{{RUN_ID},latest}}/")
    logger.info(f"RESUMEN: holdout={holdout} | threshold={thr} | goal_met={metrics['target_goal']['goal_met']}")


if __name__ == "__main__":
    main()

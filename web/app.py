"""Observatorio de Deserción Estudiantil — UNAD.

App Streamlit servida como sitio estático vía stlite (WebAssembly). Lee SOLO
agregados pre-computados por el job publish-dashboard (Ley 1581: sin microdatos).
El mismo archivo corre server-side con `streamlit run app.py` (plan A del día de
la sustentación) porque todas las rutas son relativas a data/.
"""

import json
import os

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Observatorio de Deserción UNAD", layout="wide")

PALETTE = ["#004B8D", "#FFC20E", "#5A6B7A", "#8CB8E8", "#C9A227"]


def has_data(name: str) -> bool:
    return os.path.exists(f"data/{name}")


@st.cache_data
def load_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(f"data/{name}")


@st.cache_data
def load_json(name: str):
    with open(f"data/{name}", encoding="utf-8") as fh:
        return json.load(fh)


st.title("Observatorio de Deserción Estudiantil — UNAD")

if not has_data("manifest.json"):
    st.info(
        "Aún no hay datos publicados. Ejecute el pipeline ETL "
        "(job `publish-dashboard`) para generar los agregados del tablero."
    )
    st.stop()

manifest = load_json("manifest.json")
st.caption(
    f"Datos actualizados: {manifest.get('generated_at', 'N/D')[:16]} · "
    f"Celdas con menos de {manifest.get('min_cell_threshold', 10)} estudiantes "
    "suprimidas o agrupadas (Ley 1581 — datos disociados)"
)

# ---------------------------------------------------------------- Fila de KPIs
if has_data("kpis.json"):
    kpis = load_json("kpis.json")
    cols = st.columns(min(len(kpis), 6))
    for col, k in zip(cols, kpis):
        valor = k.get("valor")
        texto = "N/D" if valor is None else f"{valor}{k.get('unidad', '')}"
        col.metric(k.get("etiqueta", k.get("id")), texto, help=k.get("definicion"))

tab_desc, tab_pred, tab_modelo = st.tabs(["📊 Descriptivo", "🔮 Predictivo", "🧠 Modelo"])

# ------------------------------------------------------------------ Descriptivo
with tab_desc:
    if has_data("desercion_por_semestre.csv"):
        df_sem = load_csv("desercion_por_semestre.csv").sort_values("semestre_orden")
        c1, c2 = st.columns(2)
        c1.plotly_chart(
            px.bar(df_sem, x="semestre_academico", y="matriculados",
                   title="Matrícula por semestre académico",
                   color_discrete_sequence=PALETTE),
            use_container_width=True)
        c2.plotly_chart(
            px.line(df_sem.dropna(subset=["tasa_desercion_pct"]),
                    x="semestre_academico", y="tasa_desercion_pct", markers=True,
                    title="Tasa de deserción intersemestral (%)",
                    color_discrete_sequence=PALETTE),
            use_container_width=True)

    c1, c2 = st.columns(2)
    if has_data("desercion_por_escuela.csv"):
        df_esc = load_csv("desercion_por_escuela.csv").dropna(subset=["tasa_desercion_pct"])
        c1.plotly_chart(
            px.bar(df_esc.sort_values("tasa_desercion_pct"),
                   x="tasa_desercion_pct", y="escuela", orientation="h",
                   title="Deserción por escuela (%)",
                   hover_data=["matriculados", "evaluables"],
                   color_discrete_sequence=PALETTE),
            use_container_width=True)
    if has_data("desercion_demografia.csv"):
        df_demo = load_csv("desercion_demografia.csv")
        etiquetas = {"sexo": "Sexo", "zona_de_residencia": "Zona de residencia",
                     "estrato_social": "Estrato social", "nivel_conectividad": "Nivel de conectividad"}
        dim = c2.selectbox("Dimensión demográfica",
                           df_demo["dimension"].unique(),
                           format_func=lambda d: etiquetas.get(d, d))
        sub = df_demo[df_demo["dimension"] == dim].dropna(subset=["tasa_desercion_pct"])
        c2.plotly_chart(
            px.bar(sub, x="categoria", y="tasa_desercion_pct",
                   title=f"Deserción por {etiquetas.get(dim, dim).lower()} (%)",
                   hover_data=["matriculados", "evaluables"],
                   color_discrete_sequence=PALETTE),
            use_container_width=True)

    if has_data("desercion_por_programa.csv"):
        df_prog = load_csv("desercion_por_programa.csv")
        st.subheader("Deserción por programa académico")
        st.dataframe(
            df_prog.sort_values("tasa_desercion_pct", ascending=False),
            use_container_width=True, hide_index=True)

    if has_data("mapa_departamento.csv"):
        df_mapa = load_csv("mapa_departamento.csv").dropna(subset=["tasa_desercion_pct"])
        st.plotly_chart(
            px.scatter(df_mapa, x="promedio_ipm_mpio", y="tasa_desercion_pct",
                       size="matriculados", hover_name="departamento_residencia",
                       title="Deserción vs. pobreza (IPM municipal promedio) por departamento",
                       labels={"promedio_ipm_mpio": "IPM municipal promedio (Sisbén IV)",
                               "tasa_desercion_pct": "Tasa de deserción (%)"},
                       color_discrete_sequence=PALETTE),
            use_container_width=True)

# ------------------------------------------------------------------- Predictivo
with tab_pred:
    if has_data("riesgo_por_programa.csv"):
        df_riesgo = load_csv("riesgo_por_programa.csv")
        sem_vigente = df_riesgo["semestre_academico"].iloc[0] if len(df_riesgo) else "N/D"
        st.caption(f"Riesgo estimado por el modelo para el semestre vigente ({sem_vigente}). "
                   "Solo se muestran agregados por programa.")
        st.plotly_chart(
            px.scatter(df_riesgo.dropna(subset=["pct_riesgo_alto"]),
                       x="n_estudiantes", y="pct_riesgo_alto",
                       color="escuela", hover_name="programa",
                       title="% de estudiantes en riesgo alto por programa",
                       labels={"n_estudiantes": "Estudiantes", "pct_riesgo_alto": "% riesgo alto"},
                       color_discrete_sequence=PALETTE),
            use_container_width=True)
        st.dataframe(
            df_riesgo.sort_values("pct_riesgo_alto", ascending=False),
            use_container_width=True, hide_index=True)
    else:
        st.info("Aún no hay predicciones publicadas: entrene el modelo "
                "(ejecución del pipeline con `\"trainModel\": true`).")

# ----------------------------------------------------------------------- Modelo
with tab_modelo:
    if has_data("metricas_modelo.json"):
        m = load_json("metricas_modelo.json")
        test = m.get("test", {})
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("F1 (holdout)", f"{test.get('f1', float('nan')):.3f}")
        c2.metric("Precisión (holdout)", f"{test.get('precision', float('nan')):.3f}")
        c3.metric("Recall (holdout)", f"{test.get('recall', float('nan')):.3f}")
        c4.metric("PR-AUC (holdout)", f"{test.get('pr_auc', float('nan')):.3f}")
        cv = m.get("cv", {})
        st.caption(
            f"Validación: holdout 20% de estudiantes nunca vistos + K-fold k=5 agrupado por estudiante · "
            f"F1 CV: {cv.get('f1_mean_default', float('nan')):.3f} ± {cv.get('f1_std_default', float('nan')):.3f} · "
            f"Umbral: {m.get('umbral', {}).get('threshold', 'N/D')} "
            f"({m.get('umbral', {}).get('criterion', '')})"
        )

        c1, c2 = st.columns(2)
        if m.get("curva_pr_test"):
            pr = pd.DataFrame(m["curva_pr_test"])
            c1.plotly_chart(
                px.line(pr, x="recall", y="precision",
                        title="Curva Precision-Recall (holdout)",
                        color_discrete_sequence=PALETTE),
                use_container_width=True)
        if has_data("importancia_variables.csv"):
            imp = load_csv("importancia_variables.csv").head(15)
            c2.plotly_chart(
                px.bar(imp.sort_values("importance_mean"),
                       x="importance_mean", y="feature", orientation="h",
                       error_x="importance_std",
                       title="Importancia de variables (permutación)",
                       color_discrete_sequence=PALETTE),
                use_container_width=True)

        if m.get("baseline_logreg_test"):
            b = m["baseline_logreg_test"]
            st.caption(
                f"Baseline (regresión logística): F1 {b.get('f1', float('nan')):.3f} · "
                f"PR-AUC {b.get('pr_auc', float('nan')):.3f} — evidencia del aporte del modelo de gradient boosting.")
        cm = test.get("confusion_matrix")
        if cm:
            st.caption(f"Matriz de confusión (holdout): TP={cm['tp']} FP={cm['fp']} "
                       f"FN={cm['fn']} TN={cm['tn']}")
    else:
        st.info("Aún no hay métricas del modelo publicadas.")

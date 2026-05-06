"""
Lambda de Ingesta de Datos Académicos UNAD (Capa Bronze del Data Lake)

Extrae microdatos académicos desde el portal UNAD para un periodo específico.

Flujo:
1. Consulta portal UNAD vía POST
2. Reintenta con backoff exponencial
3. Valida respuesta
4. Limpia metadata del portal
5. Normaliza encoding (cp1252 → UTF-8)
6. Agrega columna de trazabilidad "periodo"
7. Guarda en S3 (Bronze)
8. Emite logs y métricas en CloudWatch
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import csv
import io
import re
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# ============================================================
# CONFIGURACIÓN
# ============================================================

PROJECT_NAME = os.environ.get("PROJECT_NAME", "data-lake-academico")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

DATA_LAKE_BUCKET = os.environ.get("DATA_LAKE_BUCKET", "")
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", f"{PROJECT_NAME}/Ingestion")

SOURCE_NAME = os.environ.get("SOURCE_NAME", "unad")

DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "data")

UNAD_ENDPOINT = "https://datos.unad.edu.co/t2851.php"
UNAD_REFERER = "https://datos.unad.edu.co/datosestudiantes.php"

USER_AGENT = f"{PROJECT_NAME}-{ENVIRONMENT}-datalake-extractor/1.0"

_boto_config = Config(retries={"max_attempts": 5, "mode": "adaptive"})
_s3_client = boto3.client("s3", config=_boto_config) if not DRY_RUN else None
_cw_client = boto3.client("cloudwatch", config=_boto_config) if not DRY_RUN else None


# ============================================================
# LOGS Y MÉTRICAS
# ============================================================

def _log(event_type: str, **fields: Any) -> None:
    payload = {
        "event": event_type,
        "source": SOURCE_NAME,
        "project": PROJECT_NAME,
        "environment": ENVIRONMENT,
        **fields,
    }
    print(json.dumps(payload, default=str, ensure_ascii=False))


def _emit_metric(name: str, value: float, unit: str = "Count", **dims: str) -> None:
    if not _cw_client:
        return

    try:
        dimensions = [
            {"Name": "Source", "Value": SOURCE_NAME},
            {"Name": "Environment", "Value": ENVIRONMENT},
        ]

        for k, v in dims.items():
            dimensions.append({"Name": k, "Value": str(v)})

        _cw_client.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[
                {
                    "MetricName": name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": dimensions,
                    "Timestamp": datetime.now(timezone.utc),
                }
            ],
        )
    except (BotoCoreError, ClientError) as exc:
        _log("metric_emit_failed", metric=name, error=str(exc))


# ============================================================
# EXTRACCIÓN
# ============================================================


def _fetch_period(periodo: str, tipo: str, nivel: str, max_attempts: int = 4) -> bytes:
    """
    Extrae datos del portal UNAD y los normaliza a CSV limpio.
    """

    payload = urllib.parse.urlencode(
        {
            "r": "2851",
            "v3": tipo,
            "v4": periodo,
            "v5": nivel,
            "iformato94": "0",
            "separa": ";", # Solicitamos separador punto y coma
        }
    ).encode("utf-8")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": USER_AGENT,
        "Referer": UNAD_REFERER,
        "Accept": "text/csv,*/*;q=0.8",
    }

    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            request = urllib.request.Request(
                UNAD_ENDPOINT, data=payload, headers=headers, method="POST"
            )

            with urllib.request.urlopen(request, timeout=120) as response:
                body = response.read()

            if b"No es posible iniciar el sistema" in body:
                raise ValueError(f"UNAD rechazó el periodo {periodo}")

            if len(body) < 256:
                raise ValueError(f"Respuesta inválida UNAD periodo {periodo}")

            # ====================================================
            # 1. Encoding robusto (Prioridad a UTF-8 para evitar '√ì')
            # ====================================================
            try:
                text = body.decode("utf-8")
            except UnicodeDecodeError:
                text = body.decode("cp1252", errors="replace")

            lines = text.splitlines()

            # ====================================================
            # 2. Extraer nombre del periodo y detectar header real
            # ====================================================
            periodo_desc = periodo # Valor por defecto por si falla la extracción
            header_index = None

            for idx, line in enumerate(lines):
                # Capturar el texto: "2024 I PERIODO 16-01"
                if line.startswith("Estudiantes del periodo"):
                    match = re.search(r"Estudiantes del periodo\s+(.*?)\s*\(", line)
                    if match:
                        periodo_desc = match.group(1).strip()

                # Detectar dónde empiezan los datos reales
                low = line.lower()
                if "id" in low and "edad" in low and "sexo" in low and ";" in low:
                    header_index = idx
                    break

            if header_index is None:
                raise ValueError("No se encontró encabezado del dataset")

            data_lines = lines[header_index:]

            # ====================================================
            # 3. Parse correcto CSV (Respetando el delimitador ';')
            # ====================================================
            reader = csv.reader(data_lines, delimiter=";")

            output = io.StringIO()
            writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_MINIMAL)

            header = next(reader)

            # Limpiamos el encabezado de posibles comillas y agregamos el periodo
            header = [h.strip().strip('"') for h in header]
            header = ["periodo"] + header
            writer.writerow(header)

            for row in reader:
                if not row:
                    continue
                # Insertamos el texto extraído (ej: "2024 I PERIODO 16-01") en lugar del código "1701"
                writer.writerow([periodo_desc] + row)

            # Guardamos el CSV final asegurando UTF-8
            return output.getvalue().encode("utf-8")

        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ConnectionError, ValueError) as exc:
            last_exc = exc
            backoff = min(2 ** attempt, 30)

            _log(
                "fetch_attempt_failed",
                periodo=periodo,
                attempt=attempt,
                error=str(exc),
                retry_in_seconds=backoff,
            )

            time.sleep(backoff)

    raise RuntimeError(
        f"Fallo extracción UNAD periodo={periodo}"
    ) from last_exc


# ============================================================
# S3
# ============================================================

def _build_s3_key(periodo: str) -> str:
    """
    Nombre final del archivo en S3 (sin particiones tipo "periodo=").
    """
    safe = periodo.replace(" ", "_").replace("(", "").replace(")", "")
    return f"unad/estudiantes_periodo_{safe}.csv"


def _upload_to_s3(content: bytes, periodo: str, extracted_at_iso: str) -> str:
    key = _build_s3_key(periodo)

    _s3_client.put_object(
        Bucket=DATA_LAKE_BUCKET,
        Key=key,
        Body=content,
        ContentType="text/csv; charset=utf-8",
        ServerSideEncryption="aws:kms",
        Metadata={
            "periodo": periodo,
            "extracted_at": extracted_at_iso,
            "source": SOURCE_NAME,
        },
    )

    return key


def _save_local(content: bytes, periodo: str) -> str:
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
    path = os.path.join(LOCAL_OUTPUT_DIR, f"unad_{periodo}.csv")

    with open(path, "wb") as f:
        f.write(content)

    return path


# ============================================================
# LAMBDA
# ============================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    start = time.monotonic()
    extracted_at = datetime.now(timezone.utc).isoformat()

    periodo = str(event.get("periodo", "")).strip()
    tipo = str(event.get("tipo", "1")).strip()
    nivel = str(event.get("nivel", "2")).strip()

    if not periodo:
        raise ValueError("periodo es obligatorio")

    _log("ingestion_started", periodo=periodo)

    body = _fetch_period(periodo, tipo, nivel)
    size = len(body)

    if DRY_RUN:
        path = _save_local(body, periodo)
        return {"status": "ok", "path": path, "size": size}

    key = _upload_to_s3(body, periodo, extracted_at)

    elapsed = time.monotonic() - start

    _log(
        "ingestion_completed",
        periodo=periodo,
        key=key,
        size=size,
        seconds=round(elapsed, 2),
    )

    return {
        "status": "ok",
        "bucket": DATA_LAKE_BUCKET,
        "key": key,
        "periodo": periodo,
        "size": size,
        "elapsed": round(elapsed, 2),
    }
"""
Lambda de ingesta para Datos Abiertos Colombia (Socrata API).

Transmite un conjunto de datos desde datos.gov.co al bucket S3 Bronze (Raw),
paginando con $limit/$offset. Cada página se carga como un objeto JSON
independiente para limitar el uso de memoria y permitir la re-ingesta paralela.
Emite logs estructurados y métricas personalizadas de CloudWatch.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import concurrent.futures
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

PROJECT_NAME = os.environ.get("PROJECT_NAME", "data-lake-academico")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
DATA_LAKE_BUCKET = os.environ.get("DATA_LAKE_BUCKET", "")
METRICS_NAMESPACE = os.environ.get(
    "METRICS_NAMESPACE", f"{PROJECT_NAME}/Ingestion"
)
SOURCE_NAME = os.environ.get("SOURCE_NAME", "gobierno")
PAGE_LIMIT_DEFAULT = int(os.environ.get("PAGE_LIMIT", "50000"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "data")
TIME_BUDGET_SAFETY_MS = int(os.environ.get("TIME_BUDGET_SAFETY_MS", "60000"))

DATOS_BASE_URL = "https://www.datos.gov.co/resource"
USER_AGENT = f"{PROJECT_NAME}-{ENVIRONMENT}-datalake-extractor/1.0"

_boto_config = Config(retries={"max_attempts": 5, "mode": "adaptive"})
_s3_client = boto3.client("s3", config=_boto_config) if not DRY_RUN else None
_cw_client = boto3.client("cloudwatch", config=_boto_config) if not DRY_RUN else None


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


def _fetch_page(
    dataset_id: str, limit: int, offset: int, max_attempts: int = 4
) -> List[dict]:
    query = urllib.parse.urlencode({"$limit": limit, "$offset": offset})
    url = f"{DATOS_BASE_URL}/{dataset_id}.json?{query}"
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status != 200:
                    raise RuntimeError(
                        f"HTTP {resp.status} en dataset={dataset_id} offset={offset}"
                    )
                body = resp.read().decode("utf-8")
            data = json.loads(body)
            if not isinstance(data, list):
                raise ValueError(
                    f"Tipo de carga inesperado para dataset={dataset_id}: {type(data).__name__}"
                )
            return data
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ConnectionError, ValueError, json.JSONDecodeError) as exc:
            last_exc = exc
            backoff = min(2 ** attempt, 30)
            _log(
                "fetch_attempt_failed",
                dataset_id=dataset_id,
                offset=offset,
                attempt=attempt,
                error=str(exc),
                retry_in_seconds=backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(
        f"Extracción fallida para dataset={dataset_id} offset={offset} tras {max_attempts} intentos"
    ) from last_exc


def _build_chunk_key(dataset_id: str, extracted_at_iso: str, page: int) -> str:
    # Se mantiene la lógica original de sobreescritura si así lo deseas,
    # o puedes agregar 'page' al nombre si quieres archivos separados.
    return f"gobierno/{dataset_id}.json"


def _upload_chunk(records: List[dict], dataset_id: str, extracted_at_iso: str, page: int) -> str:
    key = _build_chunk_key(dataset_id, extracted_at_iso, page)
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")
    _s3_client.put_object(
        Bucket=DATA_LAKE_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
        ServerSideEncryption="aws:kms",
        Metadata={
            "source": SOURCE_NAME,
            "dataset-id": dataset_id,
            "extracted-at": extracted_at_iso,
            "page": str(page),
            "records-count": str(len(records)),
            "project": PROJECT_NAME,
            "environment": ENVIRONMENT,
        },
    )
    return key


def _save_local(records: List[dict], dataset_id: str, page: int) -> str:
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
    path = os.path.join(LOCAL_OUTPUT_DIR, f"local_gobierno_{dataset_id}_part-{page:07d}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False)
    return path

# --- Nueva función de apoyo para los hilos ---
def _worker_tarea(dataset_id, limit, offset, extracted_at, page):
    records = _fetch_page(dataset_id, limit, offset)
    if not records:
        return 0, None
    if DRY_RUN:
        key = _save_local(records, dataset_id, page)
    else:
        key = _upload_chunk(records, dataset_id, extracted_at, page)
    return len(records), key


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    started_monotonic = time.monotonic()
    extracted_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    dataset_id = str(event.get("dataset_id", "")).strip()
    if not dataset_id:
        _log("validation_error", message="dataset_id es requerido")
        raise ValueError("'dataset_id' es requerido en el evento de invocacion")

    limit = int(event.get("limit", PAGE_LIMIT_DEFAULT))
    offset_start = int(event.get("offset_start", 0))
    raw_max_pages = event.get("max_pages")
    max_pages = int(raw_max_pages) if raw_max_pages is not None else 10 # Por defecto 10 ráfagas

    if not DRY_RUN and not DATA_LAKE_BUCKET:
        _log("config_error", message="DATA_LAKE_BUCKET no configurada")
        raise RuntimeError("DATA_LAKE_BUCKET es requerida")

    _log(
        "ingestion_iniciada",
        dataset_id=dataset_id,
        limit=limit,
        offset_start=offset_start,
        max_pages=max_pages,
        bucket=DATA_LAKE_BUCKET if not DRY_RUN else "<dry-run>",
    )

    total_records = 0
    keys_written: List[str] = []

    try:
        # --- Lógica de hilos para paralelismo ---
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            tareas = {
                executor.submit(
                    _worker_tarea, dataset_id, limit, offset_start + (i * limit), extracted_at, i
                ): i for i in range(max_pages)
            }

            for future in concurrent.futures.as_completed(tareas):
                num_pagina = tareas[future]
                num_registros, key = future.result()
                if key:
                    total_records += num_registros
                    keys_written.append(key)
                    _log("pagina_procesada", page=num_pagina, registros=num_registros, key=key)

        elapsed = time.monotonic() - started_monotonic
        _emit_metric("IngestionRecords", total_records, DatasetId=dataset_id)
        _emit_metric("IngestionDurationSeconds", elapsed, unit="Seconds", DatasetId=dataset_id)
        _emit_metric("IngestionSuccess", 1, DatasetId=dataset_id)

        _log(
            "ingestion_completada",
            dataset_id=dataset_id,
            total_records=total_records,
            elapsed_seconds=round(elapsed, 2),
        )

        return {
            "status": "success",
            "storage": "local" if DRY_RUN else "s3",
            "source": SOURCE_NAME,
            "dataset_id": dataset_id,
            "total_records": total_records,
            "keys_written": keys_written[:5],
            "elapsed_seconds": round(elapsed, 2),
            "extracted_at": extracted_at,
        }

    except Exception as exc:
        elapsed = time.monotonic() - started_monotonic
        _emit_metric("IngestionFailure", 1, DatasetId=dataset_id)
        _log(
            "ingestion_fallida",
            dataset_id=dataset_id,
            error=str(exc),
            elapsed_seconds=round(elapsed, 2),
        )
        raise
"""
Ingestion lambda for UNAD academic microdata.

Pulls a CSV per academic period from the UNAD open data portal via POST,
validates the response, and lands the file in the Bronze (Raw) S3 bucket
under a partitioned key. Emits structured logs and CloudWatch custom metrics.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

PROJECT_NAME = os.environ.get("PROJECT_NAME", "data-lake-academico")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
DATA_LAKE_BUCKET = os.environ.get("DATA_LAKE_BUCKET", "")
METRICS_NAMESPACE = os.environ.get(
    "METRICS_NAMESPACE", f"{PROJECT_NAME}/Ingestion"
)
SOURCE_NAME = os.environ.get("SOURCE_NAME", "unad")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "data")

UNAD_ENDPOINT = "https://datos.unad.edu.co/t2851.php"
UNAD_REFERER = "https://datos.unad.edu.co/datosestudiantes.php"
USER_AGENT = f"{PROJECT_NAME}-{ENVIRONMENT}-datalake-extractor/1.0"

_boto_config = Config(retries={"max_attempts": 5, "mode": "adaptive"})
_s3_client = boto3.client("s3", config=_boto_config) if not DRY_RUN else None
_cw_client = boto3.client("cloudwatch", config=_boto_config) if not DRY_RUN else None


def _log(event_type: str, **fields: Any) -> None:
    """Emit a single-line JSON log record (CloudWatch friendly)."""
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


def _fetch_period(periodo: str, tipo: str, nivel: str, max_attempts: int = 4) -> bytes:
    payload = urllib.parse.urlencode(
        {
            "r": "2851",
            "v3": tipo,
            "v4": periodo,
            "v5": nivel,
            "iformato94": "0",
            "separa": ";",
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
                raise ValueError(
                    f"UNAD portal rejected period={periodo} (invalid parameters)"
                )
            if len(body) < 256:
                raise ValueError(
                    f"UNAD response too small for period={periodo} ({len(body)} bytes)"
                )
            return body
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
        f"UNAD extraction failed for period={periodo} after {max_attempts} attempts"
    ) from last_exc


def _build_s3_key(periodo: str, extracted_at_iso: str) -> str:
    date_part = extracted_at_iso[:10]
    yyyy, mm, dd = date_part.split("-")
    return (
        f"unad/extracted_at={date_part}/periodo={periodo}/"
        f"year={yyyy}/month={mm}/day={dd}/estudiantes.csv"
    )


def _upload_to_s3(content: bytes, periodo: str, extracted_at_iso: str) -> str:
    key = _build_s3_key(periodo, extracted_at_iso)
    _s3_client.put_object(
        Bucket=DATA_LAKE_BUCKET,
        Key=key,
        Body=content,
        ContentType="text/csv; charset=windows-1252",
        ServerSideEncryption="aws:kms",
        Metadata={
            "source": SOURCE_NAME,
            "periodo": periodo,
            "extracted-at": extracted_at_iso,
            "project": PROJECT_NAME,
            "environment": ENVIRONMENT,
        },
    )
    return key


def _save_local(content: bytes, periodo: str) -> str:
    os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
    path = os.path.join(LOCAL_OUTPUT_DIR, f"local_unad_{periodo}.csv")
    with open(path, "wb") as fh:
        fh.write(content)
    return path


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:  # noqa: ARG001
    started_monotonic = time.monotonic()
    extracted_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    periodo = str(event.get("periodo", "")).strip()
    tipo = str(event.get("tipo", "1")).strip()
    nivel = str(event.get("nivel", "2")).strip()

    if not periodo:
        _log("validation_error", message="periodo is required")
        raise ValueError("'periodo' is required in the invocation event")
    if not DRY_RUN and not DATA_LAKE_BUCKET:
        _log("config_error", message="DATA_LAKE_BUCKET env var not set")
        raise RuntimeError("DATA_LAKE_BUCKET is required when not in DRY_RUN mode")

    _log(
        "ingestion_started",
        periodo=periodo,
        tipo=tipo,
        nivel=nivel,
        bucket=DATA_LAKE_BUCKET if not DRY_RUN else "<dry-run>",
    )

    try:
        body = _fetch_period(periodo, tipo, nivel)
        size_bytes = len(body)

        if DRY_RUN:
            local_path = _save_local(body, periodo)
            _log("local_save_completed", periodo=periodo, path=local_path, size_bytes=size_bytes)
            return {
                "status": "success",
                "storage": "local",
                "source": SOURCE_NAME,
                "periodo": periodo,
                "path": local_path,
                "size_bytes": size_bytes,
            }

        key = _upload_to_s3(body, periodo, extracted_at)
        elapsed = time.monotonic() - started_monotonic

        _emit_metric("IngestionBytes", size_bytes, unit="Bytes", Periodo=periodo)
        _emit_metric("IngestionDurationSeconds", elapsed, unit="Seconds", Periodo=periodo)
        _emit_metric("IngestionSuccess", 1, Periodo=periodo)

        _log(
            "ingestion_completed",
            periodo=periodo,
            bucket=DATA_LAKE_BUCKET,
            key=key,
            size_bytes=size_bytes,
            elapsed_seconds=round(elapsed, 2),
        )
        return {
            "status": "success",
            "storage": "s3",
            "source": SOURCE_NAME,
            "periodo": periodo,
            "bucket": DATA_LAKE_BUCKET,
            "key": key,
            "size_bytes": size_bytes,
            "elapsed_seconds": round(elapsed, 2),
            "extracted_at": extracted_at,
        }

    except Exception as exc:
        elapsed = time.monotonic() - started_monotonic
        _emit_metric("IngestionFailure", 1, Periodo=periodo)
        _log(
            "ingestion_failed",
            periodo=periodo,
            error=str(exc),
            error_type=type(exc).__name__,
            elapsed_seconds=round(elapsed, 2),
        )
        raise

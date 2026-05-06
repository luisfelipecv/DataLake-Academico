"""
Ingestion lambda for Datos Abiertos Colombia (Socrata API).

Streams a single dataset from datos.gov.co into the Bronze (Raw) S3 bucket,
paginating with $limit/$offset. Each page is uploaded as an independent
JSON object to bound memory usage and enable parallel re-ingestion.
Emits structured logs and CloudWatch custom metrics.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
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
                        f"HTTP {resp.status} on dataset={dataset_id} offset={offset}"
                    )
                body = resp.read().decode("utf-8")
            data = json.loads(body)
            if not isinstance(data, list):
                raise ValueError(
                    f"Unexpected payload type for dataset={dataset_id}: {type(data).__name__}"
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
        f"Gobierno extraction failed for dataset={dataset_id} offset={offset} after {max_attempts} attempts"
    ) from last_exc


def _build_chunk_key(dataset_id: str, extracted_at_iso: str, page: int) -> str:
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


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    started_monotonic = time.monotonic()
    extracted_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    dataset_id = str(event.get("dataset_id", "")).strip()
    if not dataset_id:
        _log("validation_error", message="dataset_id is required")
        raise ValueError("'dataset_id' is required in the invocation event")

    limit = int(event.get("limit", PAGE_LIMIT_DEFAULT))
    offset_start = int(event.get("offset_start", 0))
    raw_max_pages = event.get("max_pages")
    max_pages = int(raw_max_pages) if raw_max_pages is not None else None

    if not DRY_RUN and not DATA_LAKE_BUCKET:
        _log("config_error", message="DATA_LAKE_BUCKET env var not set")
        raise RuntimeError("DATA_LAKE_BUCKET is required when not in DRY_RUN mode")

    _log(
        "ingestion_started",
        dataset_id=dataset_id,
        limit=limit,
        offset_start=offset_start,
        max_pages=max_pages,
        bucket=DATA_LAKE_BUCKET if not DRY_RUN else "<dry-run>",
    )

    page = 0
    offset = offset_start
    total_records = 0
    keys_written: List[str] = []

    try:
        while True:
            if max_pages is not None and page >= max_pages:
                _log("max_pages_reached", dataset_id=dataset_id, page=page, max_pages=max_pages)
                break
            if context is not None and hasattr(context, "get_remaining_time_in_millis"):
                remaining_ms = context.get_remaining_time_in_millis()
                if remaining_ms < TIME_BUDGET_SAFETY_MS:
                    _log(
                        "time_budget_low",
                        dataset_id=dataset_id,
                        remaining_ms=remaining_ms,
                        page=page,
                        offset=offset,
                    )
                    break

            records = _fetch_page(dataset_id, limit, offset)
            if not records:
                _log(
                    "dataset_drained",
                    dataset_id=dataset_id,
                    total_pages=page,
                    total_records=total_records,
                )
                break

            if DRY_RUN:
                key = _save_local(records, dataset_id, page)
            else:
                key = _upload_chunk(records, dataset_id, extracted_at, page)
            keys_written.append(key)

            total_records += len(records)
            page += 1
            offset += limit

            _log(
                "page_uploaded",
                dataset_id=dataset_id,
                page=page,
                records=len(records),
                total_records=total_records,
                key=key,
            )

        elapsed = time.monotonic() - started_monotonic
        _emit_metric("IngestionRecords", total_records, DatasetId=dataset_id)
        _emit_metric("IngestionPages", page, DatasetId=dataset_id)
        _emit_metric("IngestionDurationSeconds", elapsed, unit="Seconds", DatasetId=dataset_id)
        _emit_metric("IngestionSuccess", 1, DatasetId=dataset_id)

        _log(
            "ingestion_completed",
            dataset_id=dataset_id,
            total_records=total_records,
            total_pages=page,
            last_offset=offset,
            elapsed_seconds=round(elapsed, 2),
        )

        return {
            "status": "success",
            "storage": "local" if DRY_RUN else "s3",
            "source": SOURCE_NAME,
            "dataset_id": dataset_id,
            "bucket": DATA_LAKE_BUCKET if not DRY_RUN else None,
            "total_records": total_records,
            "total_pages": page,
            "last_offset": offset,
            "keys_written_sample": keys_written[:5],
            "elapsed_seconds": round(elapsed, 2),
            "extracted_at": extracted_at,
        }

    except Exception as exc:
        elapsed = time.monotonic() - started_monotonic
        _emit_metric("IngestionFailure", 1, DatasetId=dataset_id)
        _log(
            "ingestion_failed",
            dataset_id=dataset_id,
            error=str(exc),
            error_type=type(exc).__name__,
            page=page,
            total_records=total_records,
            elapsed_seconds=round(elapsed, 2),
        )
        raise

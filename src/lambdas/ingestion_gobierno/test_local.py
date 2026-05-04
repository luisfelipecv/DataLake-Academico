"""Local smoke test for the Gobierno (Datos Abiertos) ingestion lambda (DRY_RUN mode)."""
import json
import os
import time

os.environ.setdefault("DRY_RUN", "1")
os.environ.setdefault("LOCAL_OUTPUT_DIR", "data")

from lambda_function import lambda_handler  # noqa: E402


def main():
    # By default we test against Sisben IV with a single page (50K records)
    test_event = {
        "dataset_id": "hq2v-5umk",
        "limit": 50000,
        "offset_start": 0,
        "max_pages": 1,
    }
    print(f"Invoking lambda_handler with: {test_event}")
    started = time.time()
    result = lambda_handler(test_event, None)
    elapsed = time.time() - started
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    print(f"\nElapsed: {elapsed:.2f}s")


if __name__ == "__main__":
    main()

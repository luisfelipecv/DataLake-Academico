"""Local smoke test for the UNAD ingestion lambda (DRY_RUN mode)."""
import json
import os
import time

os.environ.setdefault("DRY_RUN", "1")
os.environ.setdefault("LOCAL_OUTPUT_DIR", "data")

from lambda_function import lambda_handler  # noqa: E402


def main():
    test_event = {"periodo": "2034", "tipo": "1", "nivel": "2"}
    print(f"Invoking lambda_handler with: {test_event}")
    started = time.time()
    result = lambda_handler(test_event, None)
    elapsed = time.time() - started
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"\nElapsed: {elapsed:.2f}s")
    if result.get("status") == "success":
        path = result.get("path")
        if path and os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 * 1024)
            print(f"Saved file: {path} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    main()

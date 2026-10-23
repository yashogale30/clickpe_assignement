#!/usr/bin/env python3
"""Run the S3 ingestion handler locally against a CSV and the Docker Postgres database.

Usage:
    python scripts/local_ingest.py path/to/users.csv <batch-id>

Install Lambda dependencies first: pip install -r infra/requirements.txt.
The script starts a tiny local webhook returning 204 unless --webhook-url is supplied.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lambdas import process_csv  # noqa: E402
from lambdas.db import get_connection  # noqa: E402


class LocalS3Client:
    """Minimal get_object implementation needed by the production handler."""

    def __init__(self, csv_path: Path) -> None:
        self.csv_path = csv_path

    def get_object(self, **_kwargs: object) -> dict[str, io.BufferedReader]:
        return {"Body": self.csv_path.open("rb")}


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        self.send_response(204)
        self.end_headers()

    def log_message(self, _format: str, *_args: object) -> None:
        return


def start_stub_webhook() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", 0), WebhookHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    os.environ["N8N_WEBHOOK_URL"] = f"http://127.0.0.1:{server.server_port}/webhook"
    return server


def load_local_env() -> None:
    """Read simple KEY=value entries without adding python-dotenv to the Lambda package."""
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", type=Path)
    parser.add_argument("batch_id", help="A UUID already inserted into batches")
    parser.add_argument("--webhook-url", help="Use a real n8n webhook instead of a local 204 stub")
    args = parser.parse_args()
    if not args.csv_file.is_file():
        parser.error(f"CSV file not found: {args.csv_file}")

    load_local_env()
    os.environ.setdefault("DB_HOST", "localhost")
    os.environ.setdefault("DB_PORT", "5432")
    os.environ.setdefault("DB_NAME", "loan_eligibility")
    os.environ.setdefault("DB_USER", "loan_app")
    os.environ.setdefault("DB_PASSWORD", "change_me_locally")
    os.environ.setdefault("WEBHOOK_SECRET", "local-dev-secret")
    server = None
    if args.webhook_url:
        os.environ["N8N_WEBHOOK_URL"] = args.webhook_url
    else:
        server = start_stub_webhook()

    with get_connection().transaction():
        get_connection().execute(
            "INSERT INTO batches (batch_id, status) VALUES (%s, 'uploaded') ON CONFLICT DO NOTHING",
            (args.batch_id,),
        )

    process_csv.get_s3_client = lambda: LocalS3Client(args.csv_file)  # type: ignore[method-assign]
    event = {"Records": [{"s3": {"bucket": {"name": "local-uploads"}, "object": {"key": f"uploads/{args.batch_id}.csv"}}}]}
    try:
        print(process_csv.handler(event, None))
    finally:
        if server:
            server.shutdown()


if __name__ == "__main__":
    main()

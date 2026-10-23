"""Stream, validate, and ingest CSV files uploaded to S3."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import time
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import unquote_plus
from urllib.request import Request, urlopen

import boto3
from psycopg.types.json import Jsonb

from lambdas.db import get_connection

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

REQUIRED_COLUMNS = {
    "user_id",
    "email",
    "monthly_income",
    "credit_score",
    "employment_status",
    "age",
}
EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
COPY_CHUNK_SIZE = 1_000


def get_s3_client() -> Any:
    """Kept as a function so the local test helper can provide a file-backed client."""
    return boto3.client("s3")


def batch_id_from_key(key: str) -> str:
    """Accept only the key shape emitted by get_upload_url."""
    prefix, filename = key.rsplit("/", 1)
    if prefix != "uploads" or not filename.endswith(".csv"):
        raise ValueError(f"Unexpected upload key: {key}")
    return filename.removesuffix(".csv")


def validate_row(row: dict[str, str | None]) -> tuple[tuple[Any, ...] | None, str | None]:
    """Return a normalized row or one clear rejection reason."""
    missing = [name for name in REQUIRED_COLUMNS if not (row.get(name) or "").strip()]
    if missing:
        return None, f"Missing required value(s): {', '.join(sorted(missing))}"

    email = row["email"].strip()
    if not EMAIL_PATTERN.match(email):
        return None, "Invalid email format"

    try:
        income = Decimal(row["monthly_income"].strip())
        score = int(row["credit_score"].strip())
        age = int(row["age"].strip())
    except (InvalidOperation, ValueError):
        return None, "Income, credit score, and age must be numeric"

    if not income.is_finite() or income < 0:
        return None, "monthly_income must be at least 0"
    if not 300 <= score <= 900:
        return None, "credit_score must be between 300 and 900"
    if not 18 <= age <= 100:
        return None, "age must be between 18 and 100"

    return (
        row["user_id"].strip(),
        email,
        income,
        score,
        row["employment_status"].strip(),
        age,
    ), None


def copy_valid_rows(cursor: Any, rows: list[tuple[Any, ...]], batch_id: str) -> None:
    """COPY is substantially faster than individual INSERTs for CSV-sized batches."""
    if not rows:
        return
    statement = """
        COPY staging_users
            (user_id, email, monthly_income, credit_score, employment_status, age, batch_id)
        FROM STDIN
    """
    with cursor.copy(statement) as copy:
        for row in rows:
            copy.write_row((*row, batch_id))


def reject_rows(cursor: Any, rows: list[tuple[dict[str, Any], str]], batch_id: str) -> None:
    if rows:
        cursor.executemany(
            "INSERT INTO rejected_rows (batch_id, raw_row, reason) VALUES (%s, %s, %s)",
            [(batch_id, Jsonb(row), reason) for row, reason in rows],
        )


def ingest_object(bucket: str, key: str) -> str:
    """Process a single object while keeping only one COPY-sized row buffer in memory."""
    batch_id = batch_id_from_key(key)
    response = get_s3_client().get_object(Bucket=bucket, Key=key)
    body = io.TextIOWrapper(response["Body"], encoding="utf-8-sig", newline="")
    reader = csv.DictReader(body)

    if not reader.fieldnames or not REQUIRED_COLUMNS.issubset(reader.fieldnames):
        missing = sorted(REQUIRED_COLUMNS - set(reader.fieldnames or []))
        raise ValueError(f"CSV is missing required header(s): {', '.join(missing)}")

    total = valid = rejected = 0
    valid_buffer: list[tuple[Any, ...]] = []
    rejected_buffer: list[tuple[dict[str, Any], str]] = []
    conn = get_connection()

    with conn.transaction():
        with conn.cursor() as cursor:
            for row in reader:
                total += 1
                normalized, reason = validate_row(row)
                if reason:
                    rejected += 1
                    rejected_buffer.append((row, reason))
                else:
                    valid += 1
                    valid_buffer.append(normalized)  # type: ignore[arg-type]

                if len(valid_buffer) >= COPY_CHUNK_SIZE:
                    copy_valid_rows(cursor, valid_buffer, batch_id)
                    valid_buffer.clear()
                if len(rejected_buffer) >= COPY_CHUNK_SIZE:
                    reject_rows(cursor, rejected_buffer, batch_id)
                    rejected_buffer.clear()

            copy_valid_rows(cursor, valid_buffer, batch_id)
            reject_rows(cursor, rejected_buffer, batch_id)

            # One set-based upsert replaces row-by-row application work.
            cursor.execute(
                """
                INSERT INTO users (
                    user_id, email, monthly_income, credit_score, employment_status, age, batch_id
                )
                SELECT user_id, email, monthly_income, credit_score, employment_status, age, batch_id
                FROM staging_users
                WHERE batch_id = %s
                ON CONFLICT (user_id) DO UPDATE SET
                    email = EXCLUDED.email,
                    monthly_income = EXCLUDED.monthly_income,
                    credit_score = EXCLUDED.credit_score,
                    employment_status = EXCLUDED.employment_status,
                    age = EXCLUDED.age,
                    batch_id = EXCLUDED.batch_id,
                    updated_at = NOW()
                """,
                (batch_id,),
            )
            cursor.execute("DELETE FROM staging_users WHERE batch_id = %s", (batch_id,))
            cursor.execute(
                """
                UPDATE batches
                SET status = 'parsed', total_users = %s, valid_users = %s, rejected_users = %s
                WHERE batch_id = %s
                """,
                (total, valid, rejected, batch_id),
            )

    LOGGER.info("Parsed batch %s: total=%s valid=%s rejected=%s", batch_id, total, valid, rejected)
    return batch_id


def notify_n8n(batch_id: str) -> None:
    """Post the completed batch ID to n8n with bounded exponential retries."""
    url = os.environ["N8N_WEBHOOK_URL"]
    secret = os.environ["WEBHOOK_SECRET"]
    payload = json.dumps({"batch_id": batch_id}).encode("utf-8")
    request = Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {secret}"},
        method="POST",
    )
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            with urlopen(request, timeout=10) as response:
                if 200 <= response.status < 300:
                    return
                raise RuntimeError(f"n8n responded with HTTP {response.status}")
        except Exception as exc:  # Network errors and non-2xx responses are both retryable here.
            last_error = exc
            LOGGER.warning("n8n webhook attempt %s/3 failed for batch %s: %s", attempt, batch_id, exc)
            if attempt < 3:
                time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"n8n webhook failed after 3 attempts for batch {batch_id}") from last_error


def set_batch_failed(batch_id: str) -> None:
    conn = get_connection()
    with conn.transaction():
        conn.execute("UPDATE batches SET status = 'failed' WHERE batch_id = %s", (batch_id,))


def handler(event: dict, _context: object) -> dict:
    """Handle every S3 record. Raising preserves Lambda/SQS retry and DLQ behaviour."""
    processed = []
    for record in event.get("Records", []):
        s3 = record["s3"]
        key = unquote_plus(s3["object"]["key"])
        batch_id = batch_id_from_key(key)
        try:
            ingest_object(s3["bucket"]["name"], key)
            notify_n8n(batch_id)
            processed.append(batch_id)
        except Exception:
            LOGGER.exception("CSV processing failed for batch %s", batch_id)
            set_batch_failed(batch_id)
            raise
    return {"processed_batch_ids": processed}

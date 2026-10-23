"""Create a CSV upload URL and its corresponding ingestion batch."""

from __future__ import annotations

import json
import logging
import os
from uuid import uuid4

import boto3

from lambdas.db import get_connection

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": os.environ.get("CORS_ALLOW_ORIGIN", "*"),
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "OPTIONS,POST",
}


def _response(status_code: int, body: dict) -> dict:
    return {"statusCode": status_code, "headers": CORS_HEADERS, "body": json.dumps(body)}


def handler(event: dict, _context: object) -> dict:
    """Insert a batch then issue a short-lived browser-to-S3 PUT URL."""
    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return _response(204, {})

    batch_id = str(uuid4())
    key = f"uploads/{batch_id}.csv"
    bucket = os.environ["S3_UPLOAD_BUCKET"]

    try:
        conn = get_connection()
        with conn.transaction():
            conn.execute(
                "INSERT INTO batches (batch_id, status) VALUES (%s, 'uploaded')",
                (batch_id,),
            )

        # The browser receives permission for one object only; app credentials never leave AWS.
        url = boto3.client("s3").generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": bucket, "Key": key, "ContentType": "text/csv"},
            ExpiresIn=600,
            HttpMethod="PUT",
        )
    except Exception:
        LOGGER.exception("Could not create upload URL for batch %s", batch_id)
        return _response(500, {"message": "Could not create upload URL"})

    return _response(
        201,
        {"batch_id": batch_id, "upload_url": url, "expires_in_seconds": 600},
    )

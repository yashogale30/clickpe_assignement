"""Return one batch's processing status and funnel counters for the upload UI."""

from __future__ import annotations

import json
import logging
import os
from uuid import UUID

from lambdas.db import get_connection

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": os.environ.get("CORS_ALLOW_ORIGIN", "*"),
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
}


def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body),
    }


def handler(event: dict, _context: object) -> dict:
    """Look up a batch by UUID without exposing any other batch records."""
    if event.get("httpMethod") == "OPTIONS":
        return response(204, {})

    batch_id = (event.get("pathParameters") or {}).get("batch_id")
    try:
        UUID(str(batch_id))
    except (TypeError, ValueError):
        return response(400, {"message": "batch_id must be a UUID"})

    try:
        conn = get_connection()
        row = conn.execute(
            """
            SELECT json_build_object(
                'batch_id', batch_id,
                'status', status,
                'total_users', total_users,
                'valid_users', valid_users,
                'rejected_users', rejected_users,
                'after_range_join', after_range_join,
                'matched_count', matched_count,
                'near_miss_count', near_miss_count,
                'emailed_count', emailed_count,
                'created_at', created_at,
                'updated_at', updated_at
            )
            FROM batches
            WHERE batch_id = %s
            """,
            (batch_id,),
        ).fetchone()
    except Exception:
        LOGGER.exception("Could not get status for batch %s", batch_id)
        return response(500, {"message": "Could not fetch batch status"})

    if row is None:
        return response(404, {"message": "Batch not found"})

    return response(200, row[0])

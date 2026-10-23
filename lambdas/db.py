"""Small, reusable Postgres connection helper for Lambda handlers."""

from __future__ import annotations

import logging
import os

import psycopg

LOGGER = logging.getLogger(__name__)
_connection: psycopg.Connection | None = None


def get_connection() -> psycopg.Connection:
    """Return a warm Lambda connection, reconnecting when it is no longer usable."""
    global _connection

    if _connection is not None and not _connection.closed:
        try:
            _connection.execute("SELECT 1")
            return _connection
        except psycopg.Error:
            LOGGER.warning("Cached database connection is stale; reconnecting")
            _connection.close()

    _connection = psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
        # Health checks must not leave an implicit transaction open between warm invocations.
        # Each caller explicitly uses conn.transaction() for its atomic database work.
        autocommit=True,
    )
    return _connection

import os
import time

import psycopg2
from psycopg2 import pool

# ---------------------------------------------------------------------------
# Connection pooling (M5)
# ---------------------------------------------------------------------------
_pool: pool.SimpleConnectionPool | None = None
_override_db_url: str | None = None


def set_db_url(url: str | None) -> None:
    """Override the database URL used by get_pool(). Pass None to revert to env var."""
    global _override_db_url
    _override_db_url = url


def get_pool() -> pool.SimpleConnectionPool:
    """Return the module-level connection pool, creating it on first call."""
    global _pool
    if _pool is None or _pool.closed:
        if _override_db_url is not None:
            db_url = _override_db_url
        else:
            db_url = os.environ.get(
                "READER_DATABASE_URL",
                os.environ["DATABASE_URL"].replace(
                    "postgresql://postgres:postgres@",
                    "postgresql://cricket_reader:readonlypass@",
                ),
            )
        for attempt in range(10):
            try:
                _pool = pool.SimpleConnectionPool(minconn=1, maxconn=5, dsn=db_url)
                return _pool
            except psycopg2.OperationalError:
                if attempt == 9:
                    raise
                time.sleep(2)
    return _pool


def get_conn():
    """Get a connection from the pool."""
    return get_pool().getconn()


def put_conn(conn):
    """Return a connection to the pool."""
    p = get_pool()
    p.putconn(conn)


def release_pool():
    """Close all connections in the pool."""
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
        _pool = None


# Legacy helper kept for CLI commands (ingest, verify)
def get_connection(database_url: str, retries: int = 10, delay: int = 2):
    """Connect to PostgreSQL with retry loop for Docker startup timing."""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(database_url)
            return conn
        except psycopg2.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(delay)

from __future__ import annotations

from typing import Optional
import psycopg
from psycopg.rows import dict_row


# =========================================================
# GLOBAL CONNECTION HOLDER
# =========================================================

_CONN: Optional["PTKConnection"] = None

# =========================================================
# CONNECTION CLASS
# =========================================================

class PTKConnection:
    def __init__(self, host: str, db: str, user: str, password: str, port: int = 5432):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port

    def _conn_string(self) -> str:
        return (
            f"host={self.host} "
            f"dbname={self.db} "
            f"user={self.user} "
            f"password={self.password} "
            f"port={self.port}"
        )

    def connect(self):
        return psycopg.connect(
            self._conn_string(),
            row_factory=dict_row
        )


# =========================================================
# PUBLIC CONNECT FUNCTION
# =========================================================

def ptk_connect(host: str, db: str, user: str, password: str) -> None:
    """
    Initializes global connection object.
    """
    global _CONN
    _CONN = PTKConnection(host, db, user, password)


# The four secrets a Lakebase connection needs, named the way ptk_connect takes them.
_LAKEBASE_SECRET_FIELDS = ("host", "db", "user", "password")

# Prefix for those secrets. The same in every environment: dev used to carry the
# environment in the name (dbx-lakebase-dev-host) and was renamed to match qa and prod,
# so the scope decides which vault is read but never how the keys are spelled.
_LAKEBASE_KEY_PREFIX = "dbx-lakebase"


def fetch_lakebase_credentials() -> dict:
    """
    Reads the Lakebase connection secrets for the workspace this job runs in.

    The scope comes from the workspace, the same way the peppers resolve theirs, and the
    key names are the same in every environment — so a notebook never has to know which
    environment it is running in. Returns the secrets keyed the way ptk_connect takes them:
    ``ptk_connect(**fetch_lakebase_credentials())``.
    """
    from ..idr.hmac_util import fetch_secret_scope

    try:
        from databricks.sdk.runtime import dbutils  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "Reading the Lakebase secrets requires databricks.sdk.runtime.dbutils. "
            "Outside Databricks, build the credentials yourself and pass them to ptk_connect()."
        ) from exc

    scope = fetch_secret_scope()

    credentials = {}
    for field in _LAKEBASE_SECRET_FIELDS:
        key = f"{_LAKEBASE_KEY_PREFIX}-{field}"
        try:
            credentials[field] = dbutils.secrets.get(scope=scope, key=key)
        except Exception as exc:
            # Almost always a key that exists in one environment but not in another,
            # so name both the scope and the key instead of failing on connect.
            raise RuntimeError(
                f"Lakebase secret {key!r} is missing from scope {scope!r}."
            ) from exc

    return credentials


def _get_conn() -> PTKConnection:
    if _CONN is None:
        raise Exception("❌ Not connected. Call ptk_connect() first.")
    return _CONN


# =========================================================
# INTERNAL HELPER (used by other file)
# =========================================================

def _ptk_get_connection():
    """
    Exposed helper for other modules.
    Returns active connection object.
    """
    return _get_conn()

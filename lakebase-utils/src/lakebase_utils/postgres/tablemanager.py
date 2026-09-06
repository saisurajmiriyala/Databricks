import pandas as pd

from lakebase_utils.postgres.connection import _ptk_get_connection


# =========================================================
# PTK TABLE EXISTS
# =========================================================

def ptk_table_exists(table: str, schema: str = "public") -> bool:
    """
    Check whether a table exists.
    """

    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        ) AS exists;
    """

    conn_obj = _ptk_get_connection()

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return cur.fetchone()["exists"]


# =========================================================
# PTK TRUNCATE TABLE
# =========================================================

def ptk_truncate(table: str) -> None:
    """
    Truncate a table.
    """

    conn_obj = _ptk_get_connection()

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")

        conn.commit()

    print(f"✔ Table '{table}' truncated successfully.")


# =========================================================
# PTK DROP TABLE
# =========================================================

def ptk_drop_table(table: str, cascade: bool = False) -> None:
    """
    Drop a table if it exists.
    """

    sql = f"DROP TABLE IF EXISTS {table}"

    if cascade:
        sql += " CASCADE"

    conn_obj = _ptk_get_connection()

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

        conn.commit()

    print(f"✔ Table '{table}' dropped successfully.")


# =========================================================
# PTK ROW COUNT
# =========================================================

def ptk_row_count(table: str) -> int:
    """
    Return the number of rows in a table.
    """
    conn_obj = _ptk_get_connection()

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS row_count FROM {table}")
            return cur.fetchone()["row_count"]

# =========================================================
# PTK COPY TABLE
# =========================================================

def ptk_copy_table(
    source_table: str,
    destination_table: str,
    include_data: bool = True,
) -> None:
    """
    Copy a table structure with optional data.
    """

    conn_obj = _ptk_get_connection()

    sql = (
        f"CREATE TABLE {destination_table} AS "
        f"SELECT * FROM {source_table}"
    )

    if not include_data:
        sql += " WHERE FALSE"

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

        conn.commit()

    if include_data:
        print(
            f"✔ Table '{destination_table}' created with data from '{source_table}'."
        )
    else:
        print(
            f"✔ Empty copy of '{source_table}' created as '{destination_table}'."
        )

from __future__ import annotations
import pandas as pd
from io import StringIO
from lakebase_utils.postgres.connection import _ptk_get_connection

# =========================================================
# PTK EXECUTE
# =========================================================

def ptk_execute(sql: str, params: tuple | dict | None = None):
    """
    SELECT → returns DataFrame
    DML → prints rows affected
    """

    conn_obj = _ptk_get_connection()

    sql_clean = sql.strip().lower()
    action = sql_clean.split()[0]

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:

            cur.execute(sql, params)

            # -------------------------
            # SELECT → DataFrame
            # -------------------------
            if action == "select":
                rows = cur.fetchall()
                return pd.DataFrame(rows)

            # -------------------------
            # DML → row count
            # -------------------------
            affected = cur.rowcount
            conn.commit()

            print(f"✔ {action.upper()} completed. Rows affected: {affected}")

            return affected


# =========================================================
# PTK FETCH ONE
# =========================================================

def ptk_fetchone(sql: str, params: tuple | dict | None = None):
    """
    Fetch single row
    """

    conn_obj = _ptk_get_connection()

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


# =========================================================
# PTK FETCH ALL
# =========================================================

def ptk_fetchall(sql: str, params: tuple | dict | None = None):
    """
    Fetch all rows
    """

    conn_obj = _ptk_get_connection()

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


# =========================================================
# PTK READ DATAFRAME
# =========================================================

def ptk_read_dataframe(sql: str, params: tuple | dict | None = None):
    """
    Return query result as pandas DataFrame
    """

    conn_obj = _ptk_get_connection()

    with conn_obj.connect() as conn:
        return pd.read_sql_query(sql, conn, params=params)

# =========================================================
# PTK transform spark dataframe to pandas dataframe
# =========================================================

def ptk_auto_dataframe_converter(df):
    """
    Converts Spark / Polars / Arrow / Pandas → Pandas DataFrame
    """

    import pandas as pd

    # Already pandas
    if isinstance(df, pd.DataFrame):
        return df

    # PySpark
    if hasattr(df, "toPandas"):
        return df.toPandas()

    # Polars
    try:
        import polars as pl
        if isinstance(df, pl.DataFrame):
            return df.to_pandas()
    except Exception:
        pass

    # PyArrow
    try:
        import pyarrow as pa
        if isinstance(df, pa.Table):
            return df.to_pandas()
    except Exception:
        pass

    raise TypeError(f"Unsupported dataframe type: {type(df)}")


# =========================================================
# PTK WRITE DATAFRAME (FAST COPY)
# =========================================================

def ptk_write_dataframe(df, table: str):
    """
    Fast bulk insert using PostgreSQL COPY
    (Supports Spark / Polars / Pandas automatically)
    """

    df = ptk_auto_dataframe_converter(df)
    
    conn_obj = _ptk_get_connection()

    buffer = StringIO()

    df.to_csv(
        buffer,
        index=False,
        header=False
    )

    buffer.seek(0)

    columns = ", ".join(df.columns)

    copy_sql = f"COPY {table} ({columns}) FROM STDIN WITH CSV"

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                while data := buffer.read(8192):
                    copy.write(data)

        conn.commit()

    print(f"✔ INSERT completed. Rows inserted: {len(df)}")


# =========================================================
# PTK UPSERT DATAFRAME (FAST COPY)
# =========================================================

def ptk_upsert_dataframe(
    df: pd.DataFrame,
    table: str,
    key_columns: list[str],
):
    """
    Upsert a DataFrame into a PostgreSQL table.

    Parameters
    ----------
    df : pandas.DataFrame
        Data to upsert.
    table : str
        Target table.
    key_columns : list[str]
        Columns used in the ON CONFLICT clause.
    """

    if df.empty:
        print("No rows to upsert.")
        return

    conn_obj = _ptk_get_connection()

    temp_table = f"tmp_{uuid.uuid4().hex[:8]}"

    columns = list(df.columns)
    column_list = ", ".join(columns)

    conflict_columns = ", ".join(key_columns)

    update_columns = [
        col for col in columns
        if col not in key_columns
    ]

    update_clause = ", ".join(
        f"{col} = EXCLUDED.{col}"
        for col in update_columns
    )

    buffer = StringIO()
    df.to_csv(
        buffer,
        index=False,
        header=False,
    )
    buffer.seek(0)

    with conn_obj.connect() as conn:
        with conn.cursor() as cur:

            # Create temporary table
            cur.execute(
                f"""
                CREATE TEMP TABLE {temp_table}
                (LIKE {table} INCLUDING DEFAULTS)
                ON COMMIT DROP;
                """
            )

            # COPY dataframe into temp table
            copy_sql = (
                f"COPY {temp_table} ({column_list}) "
                "FROM STDIN WITH CSV"
            )

            with cur.copy(copy_sql) as copy:
                while data := buffer.read(8192):
                    copy.write(data)

            # Upsert into destination table
            cur.execute(
                f"""
                INSERT INTO {table} ({column_list})
                SELECT {column_list}
                FROM {temp_table}
                ON CONFLICT ({conflict_columns})
                DO UPDATE
                SET {update_clause};
                """
            )

        conn.commit()

    print(f"✔ UPSERT completed. Rows processed: {len(df)}")

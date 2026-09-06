"""Bulk transfer between Unity Catalog and Lakebase Postgres.

Two directions, both built on Postgres ``COPY`` so neither one inserts row by row:

- :func:`load_uc_to_lakebase` -- a Unity Catalog table into a Postgres table.
  Every Spark partition opens its own connection and streams a single ``COPY``,
  so the write is distributed across executors instead of funnelling through the
  driver.
- :func:`load_lakebase_to_uc` -- a Postgres table (or query) into a Unity Catalog
  table, via ``COPY ... TO STDOUT (FORMAT BINARY)`` staged as Parquet in a Volume
  and then registered as Delta with a single Spark read.

Why not the existing helpers
----------------------------
``queryhub.ptk_write_dataframe`` collects the whole DataFrame to the driver with
``toPandas()`` and rebuilds it as CSV. That is fine for the few thousand staging
rows the incremental notebooks write, and it falls over on a historical load: the
driver has to hold the entire table, pandas coerces ``int`` columns containing
NULLs into floats, and ``datetime64[ns]`` cannot represent the year-9999 sentinel
that SCD2 sources use to mark an open interval.

``mapInArrow`` avoids all three. Arrow hands back native Python objects per
column, so integers with NULLs stay ``int``/``None`` and timestamps stay
``datetime``/``None`` whatever the year. No casts, no coercion, no transformation:
what is in Delta is what lands in Postgres.

Measured throughput on the dev instance
---------------------------------------
Writing into an INDEXED table (PTK-90): 26,000-33,000 rows/s, i.e. 52M rows in
~34 min. The ceiling is the Postgres instance maintaining the index on every
insert, not the client, so raising ``repartition`` past ~32 does not help.

Writing into a table with NO indexes (PRV-98, 2M-row slices): 51,000-105,000
rows/s. Two to three times faster, which is why the historical load creates its
target tables bare and adds the indexes afterwards, where they build in bulk.

Reading out (single-stream binary COPY plus Arrow plus Parquet): 53,000-74,000
rows/s. The export runs on the driver, so it does not scale with the cluster.
"""

from __future__ import annotations

import os
import time
from typing import Any, Sequence

from lakebase_utils.postgres.connection import _ptk_get_connection

# A historical load holds a single COPY open for its whole duration, so this has
# to be generous. Measured on 2M-row slices, exporting user_identifier's 94.5M rows
# takes ~29 minutes, and that estimate is optimistic because the slice only read
# the first 2% of a 32 GB table. A 30 minute limit would abort the real run.
#
# Four hours, then. The limit still exists on purpose: without a statement_timeout
# a stalled COPY cannot even be interrupted (PTK-87). It should never be what kills
# a legitimate load, only a hung one.
DEFAULT_STATEMENT_TIMEOUT_MS = 14_400_000

# Past ~24 partitions the dev instance is the bottleneck, not the client.
DEFAULT_REPARTITION = 32

# Rows buffered on the driver before a Parquet chunk is written. A 1M chunk was
# measured working on user_identifier (13 columns, 64-char HMACs), but this is a
# library function and the driver holds every buffered row as Python objects, so
# a wide table or large payloads would run it out of memory. 250k keeps the peak
# well inside a driver at any realistic row width; callers with known-small rows
# can raise it. More chunks only means more Parquet files, which costs nothing.
DEFAULT_CHUNK_ROWS = 250_000


class BulkLoadError(RuntimeError):
    """Raised when a transfer does not land the number of rows it read."""


# =========================================================
# CONNECTION STRING
# =========================================================

def _conninfo(statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS) -> str:
    """Build a libpq connection string from the global ``ptk_connect`` state.

    Returns a plain string on purpose: it has to be serialised out to the Spark
    executors, and the ``PTKConnection`` object is not meant to travel. Built with
    ``make_conninfo`` so passwords containing spaces or quotes are escaped.
    """
    from psycopg.conninfo import make_conninfo

    conn = _ptk_get_connection()
    return make_conninfo(
        host=conn.host,
        dbname=conn.db,
        user=conn.user,
        password=conn.password,
        port=conn.port,
        sslmode="require",
        options=f"-c statement_timeout={statement_timeout_ms}",
    )


def _quote_ident(name: str) -> str:
    """Quote an identifier so mixed-case columns such as ``__START_AT`` survive."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _scalar(sql: str) -> Any:
    """Run a one-value query through the shared ``ptk_connect`` connection.

    Goes through ``ptk_fetchone`` rather than opening its own connection: this
    always runs on the driver, where the ``ptk_connect`` state exists, so there is
    no reason to duplicate connection handling here. ``PTKConnection`` builds its
    cursors with ``dict_row``, so the single value comes out of the mapping rather
    than by position.
    """
    from lakebase_utils.postgres.queryhub import ptk_fetchone

    row = ptk_fetchone(sql)
    if not row:
        return None
    return next(iter(row.values())) if isinstance(row, dict) else row[0]


# =========================================================
# UNITY CATALOG -> LAKEBASE
# =========================================================

def load_uc_to_lakebase(
    source_table: str,
    target_table: str,
    *,
    columns: Sequence[str] | None = None,
    mode: str = "truncate",
    repartition: int = DEFAULT_REPARTITION,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    verify: bool = True,
    source_df: Any = None,
) -> dict:
    """Bulk load a Unity Catalog table into a Lakebase Postgres table.

    Parameters
    ----------
    source_table:
        Fully qualified Unity Catalog table, e.g. ``dev_raw.privacy.idr_hist_user_identity``.
        Ignored when ``source_df`` is given, but still recorded in the result.
    target_table:
        Schema-qualified Postgres table, e.g. ``cg_identity.user_identity``.
    columns:
        Columns to transfer, in order. Defaults to every column of the source.
        Pass an explicit list to skip columns the database generates itself, such
        as the IDENTITY column ``membership_history_id``.
    mode:
        ``truncate`` (default) empties the target first, ``delete`` does the same
        with ``DELETE`` for targets where TRUNCATE is not granted, and ``append``
        leaves existing rows alone.
    repartition:
        Number of concurrent COPY streams. One connection per partition.
    verify:
        Compare the source count against the target count and raise on mismatch.
    source_df:
        Optional Spark DataFrame to load instead of reading ``source_table``.
        Use it when the rows are computed rather than persisted.

    Returns
    -------
    dict with the row counts, elapsed seconds and throughput.
    """
    from pyspark.sql import SparkSession

    from lakebase_utils.postgres.queryhub import ptk_execute

    if mode not in ("truncate", "delete", "append"):
        raise ValueError(f"mode must be truncate, delete or append; got {mode!r}")

    spark = SparkSession.builder.getOrCreate()
    sdf = source_df if source_df is not None else spark.table(source_table)

    cols = list(columns) if columns else list(sdf.columns)
    missing = [c for c in cols if c not in sdf.columns]
    if missing:
        raise ValueError(f"columns not present in the source: {missing}")
    sdf = sdf.select([sdf[c] for c in cols])

    pg_cols = ", ".join(_quote_ident(c) for c in cols)
    conninfo = _conninfo(statement_timeout_ms)
    copy_sql = f"COPY {target_table} ({pg_cols}) FROM STDIN"

    def copy_partition(batches):
        """Runs on the executor: one connection, one COPY, one partition.

        This is the one place that cannot go through ``ptk_connect``. The global
        it sets lives in the driver process; the executor runs in a different
        process on a different machine and has no such state. What does travel is
        ``conninfo``, a plain string built from that same ``ptk_connect`` state by
        ``_conninfo`` above, so the credentials are still owned in one place. The
        raw connection is also required for ``cur.copy()``, which the ptk helpers
        do not expose.
        """
        import psycopg
        import pyarrow as pa

        written = 0
        conn = psycopg.connect(conninfo)
        try:
            with conn.cursor() as cur:
                # The load is restartable from the source, so trading the fsync
                # guarantee for throughput is safe here.
                cur.execute("SET synchronous_commit = off;")
            with conn.cursor() as cur, cur.copy(copy_sql) as cp:
                for batch in batches:
                    # to_pylist() per column keeps native Python types: ints with
                    # NULLs stay ints, timestamps stay datetimes at any year.
                    values = [c.to_pylist() for c in batch.columns]
                    for i in range(batch.num_rows):
                        cp.write_row(tuple(v[i] for v in values))
                        written += 1
            conn.commit()
        finally:
            conn.close()
        yield pa.RecordBatch.from_pydict({"n": [written]})

    result = {
        "direction": "uc_to_lakebase",
        "source": source_table,
        "target": target_table,
        "columns": len(cols),
        "mode": mode,
        "repartition": repartition,
    }

    print(f"[bulkload] {source_table} -> {target_table} ({len(cols)} columns, mode={mode})")

    if mode != "append":
        statement = "TRUNCATE" if mode == "truncate" else "DELETE FROM"
        ptk_execute(f"{statement} {target_table}")
        print(f"[bulkload] target emptied with {statement}")

    # Counting the source up front costs a second Spark action, and the source
    # plan really is evaluated twice: the count runs on ``sdf`` while the write
    # runs on ``sdf.repartition(...)``, so the repartition shuffle exists only in
    # the second action and leaves nothing for the count to hand over.
    #
    # It is kept anyway, for two reasons. It makes the load observable, printing
    # how many rows are about to move instead of going silent until the COPY ends,
    # and it is the number ``verify`` below compares the target against. Cache or
    # persist ``sdf`` before calling if its plan is expensive to recompute.
    rows_source = sdf.count()
    result["rows_source"] = rows_source
    print(f"[bulkload] source rows: {rows_source:,}")

    started = time.time()
    written = (
        sdf.repartition(repartition)
        .mapInArrow(copy_partition, schema="n long")
        .agg({"n": "sum"})
        .collect()[0][0]
    )
    elapsed = time.time() - started

    rows_written = int(written or 0)
    result["rows_written"] = rows_written
    result["seconds"] = round(elapsed, 1)
    result["rows_per_sec"] = round(rows_source / elapsed) if elapsed else None
    print(
        f"[bulkload] wrote {result['rows_written']:,} rows in "
        f"{result['seconds']}s ({result['rows_per_sec']:,} rows/s)"
    )

    target_count = _scalar(f"SELECT count(*) FROM {target_table}")
    result["rows_target"] = target_count
    print(f"[bulkload] target now holds {target_count:,} rows")

    # Three numbers have to agree: what the source held, what the executors say
    # they wrote, and what the table now holds. On append the last one cannot be
    # compared, since the table already had rows before this call.
    if verify:
        if mode != "append" and target_count != rows_source:
            raise BulkLoadError(
                f"{target_table} holds {target_count:,} rows, expected {rows_source:,} "
                f"from {source_table}. The load is NOT complete; truncate and retry "
                f"(mapInArrow is not atomic across partitions, so a failed run can "
                f"leave rows behind)."
            )
        if rows_written != rows_source:
            raise BulkLoadError(
                f"executors reported {rows_written:,} rows written but the source "
                f"had {rows_source:,}"
            )

    result["ok"] = True
    return result


# =========================================================
# LAKEBASE -> UNITY CATALOG
# =========================================================

# Postgres type -> Arrow type. Deliberately explicit: an unmapped type raises
# instead of being silently stringified, so a new column type is a loud failure
# rather than a quiet change of meaning.
def _arrow_type(pg_type: str, precision: int | None, scale: int | None):
    import pyarrow as pa

    simple = {
        "bool": pa.bool_(),
        "int2": pa.int16(),
        "int4": pa.int32(),
        "int8": pa.int64(),
        "float4": pa.float32(),
        "float8": pa.float64(),
        "text": pa.string(),
        "varchar": pa.string(),
        "bpchar": pa.string(),
        "name": pa.string(),
        "uuid": pa.string(),
        "json": pa.string(),
        "jsonb": pa.string(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "bytea": pa.binary(),
    }
    if pg_type in simple:
        return simple[pg_type]
    if pg_type == "numeric":
        # An unconstrained numeric has no precision; Delta needs one.
        return pa.decimal128(precision or 38, scale or 18)
    raise BulkLoadError(
        f"no Arrow mapping for Postgres type {pg_type!r}. Add it to _arrow_type() "
        f"rather than letting the column be coerced."
    )


def _to_arrow_value(value):
    """Normalise the few psycopg objects Arrow does not take as-is."""
    import uuid as _uuid

    if isinstance(value, _uuid.UUID):
        return str(value)
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def load_lakebase_to_uc(
    source_table: str,
    target_table: str,
    *,
    staging_dir: str,
    columns: Sequence[str] | None = None,
    where: str | None = None,
    limit: int | None = None,
    mode: str = "overwrite",
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    verify: bool = True,
) -> dict:
    """Export a Lakebase Postgres table into a Unity Catalog table.

    Reads with ``COPY ... TO STDOUT (FORMAT BINARY)``, writes Parquet chunks into
    ``staging_dir`` (a Unity Catalog Volume path), then registers the whole
    directory as a Delta table with one Spark read. Binary COPY keeps the server
    from formatting every value as text and keeps NULL unambiguous, which a CSV
    round trip does not.

    The COPY itself is a single stream on the driver, so this direction is slower
    than the write direction. It is bounded by the Postgres side, not by memory:
    rows are flushed to Parquet every ``chunk_rows``.

    Parameters
    ----------
    source_table:
        Schema-qualified Postgres table, e.g. ``cg_identity.user_identifier``.
    target_table:
        Fully qualified Unity Catalog table to create or replace.
    staging_dir:
        Volume directory for the Parquet chunks, e.g.
        ``/Volumes/dev_raw/privacy/idr_staging/user_identifier``. Created if
        missing; any ``.parquet`` files already there are removed first.
    columns:
        Columns to export, in order. Defaults to every column.
    where:
        Optional SQL predicate, without the ``WHERE`` keyword.
    limit:
        Stop after this many rows. For smoke tests only, so the plumbing can be
        proven on a small slice before moving a 94M-row table.
    mode:
        Spark write mode for the target table: ``overwrite`` or ``append``.
    verify:
        Compare the Postgres row count against the Delta row count and raise on
        mismatch.
    """
    import psycopg
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql import SparkSession

    if mode not in ("overwrite", "append"):
        raise ValueError(f"mode must be overwrite or append; got {mode!r}")
    # The buffer flushes on ``len(buffer) >= chunk_rows``, so a zero or negative
    # threshold never fires and the driver accumulates the whole table.
    if chunk_rows < 1:
        raise ValueError(f"chunk_rows must be at least 1; got {chunk_rows!r}")

    spark = SparkSession.builder.getOrCreate()
    conninfo = _conninfo(statement_timeout_ms)

    schema_name, _, table_name = source_table.rpartition(".")
    if not schema_name:
        raise ValueError("source_table must be schema-qualified, e.g. cg_identity.user_identity")

    result = {
        "direction": "lakebase_to_uc",
        "source": source_table,
        "target": target_table,
        "staging_dir": staging_dir,
        "mode": mode,
    }
    print(f"[bulkload] {source_table} -> {target_table}")

    # A raw connection, because the export streams ``COPY ... TO STDOUT (FORMAT
    # BINARY)`` through ``cur.copy()`` and the ptk helpers do not expose the copy
    # protocol. The catalog lookup below reuses this connection rather than
    # opening a second one through ``ptk_execute``; the row count does go through
    # ``_scalar``/``ptk_fetchone``, since it is an ordinary query.
    with psycopg.connect(conninfo, autocommit=True) as conn:
        # Column list and types straight from the catalog, so the Arrow schema
        # mirrors the table instead of being guessed from the data.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, udt_name, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema_name, table_name),
            )
            catalog = cur.fetchall()
        if not catalog:
            raise BulkLoadError(f"{source_table} has no columns in information_schema")

        by_name = {row[0]: row for row in catalog}
        names = list(columns) if columns else [row[0] for row in catalog]
        unknown = [c for c in names if c not in by_name]
        if unknown:
            raise ValueError(f"columns not present in {source_table}: {unknown}")

        pg_type_names = [by_name[c][1] for c in names]
        arrow_schema = pa.schema(
            [
                pa.field(c, _arrow_type(by_name[c][1], by_name[c][2], by_name[c][3]))
                for c in names
            ]
        )

        predicate = f" WHERE {where}" if where else ""
        clause = predicate + (f" LIMIT {int(limit)}" if limit else "")
        source_count = _scalar(
            f"SELECT count(*) FROM (SELECT 1 FROM {source_table}{clause}) AS s"
        )
        result["rows_source"] = source_count
        if limit:
            result["limit"] = int(limit)
        print(f"[bulkload] source rows: {source_count:,}")

        # Clear the whole tree, not just the top level. A previous run, or a Spark
        # write that landed in the same path, can leave partition subdirectories
        # behind, and spark.read.parquet(staging_dir) reads those too: the export
        # would mix stale rows into the new table without failing.
        os.makedirs(staging_dir, exist_ok=True)
        for root, _dirs, files in os.walk(staging_dir):
            for stale in files:
                if stale.endswith(".parquet"):
                    os.remove(os.path.join(root, stale))

        select_cols = ", ".join(_quote_ident(c) for c in names)
        copy_sql = (
            f"COPY (SELECT {select_cols} FROM {source_table}{clause}) "
            f"TO STDOUT (FORMAT BINARY)"
        )

        read = 0
        chunk_index = 0
        buffer: list[tuple] = []
        started = time.time()

        def flush(rows: list[tuple], index: int) -> None:
            columnar = list(zip(*rows)) if rows else [() for _ in names]
            table = pa.table(
                {
                    name: pa.array(
                        [_to_arrow_value(v) for v in column],
                        type=arrow_schema.field(name).type,
                    )
                    for name, column in zip(names, columnar)
                },
                schema=arrow_schema,
            )
            pq.write_table(
                table, os.path.join(staging_dir, f"part-{index:05d}.parquet")
            )

        with conn.cursor() as cur, cur.copy(copy_sql) as cp:
            # set_types tells psycopg how to decode the binary stream; without it
            # binary COPY yields raw bytes.
            cp.set_types(pg_type_names)
            for row in cp.rows():
                buffer.append(row)
                read += 1
                if len(buffer) >= chunk_rows:
                    flush(buffer, chunk_index)
                    print(f"[bulkload] staged {read:,} rows")
                    buffer = []
                    chunk_index += 1
            if buffer:
                flush(buffer, chunk_index)
            elif read == 0:
                # An empty source still has to produce a file: with nothing on
                # disk, spark.read.parquet cannot infer a schema and the export
                # fails instead of writing an empty table. flush() with no rows
                # emits the columns from arrow_schema and no data.
                flush([], chunk_index)

        elapsed = time.time() - started
        result["rows_read"] = read
        result["seconds_export"] = round(elapsed, 1)
        result["rows_per_sec"] = round(read / elapsed) if elapsed else None
        print(
            f"[bulkload] exported {read:,} rows in {result['seconds_export']}s "
            f"({result['rows_per_sec']:,} rows/s)"
        )

    sdf = spark.read.parquet(staging_dir)
    sdf.write.mode(mode).saveAsTable(target_table)
    target_count = spark.table(target_table).count()
    result["rows_target"] = target_count
    print(f"[bulkload] {target_table} now holds {target_count:,} rows")

    if verify and mode == "overwrite" and target_count != source_count:
        raise BulkLoadError(
            f"{target_table} holds {target_count:,} rows, expected {source_count:,} "
            f"from {source_table}"
        )

    result["ok"] = True
    return result

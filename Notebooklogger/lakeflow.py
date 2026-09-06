import logging
import sys
import io
import time
import inspect
import os
import builtins
from datetime import datetime
from IPython import get_ipython
from lakebase_utils.idr.hmac_util import fetch_secret_scope

_current_log_path = None
_logger_already_initialized = False
LOG_SPARK_ROW_COUNTS = False  # set True only if you're OK triggering a Spark job every cell


class VolumeSafeFileHandler(logging.Handler):
    """Unity Catalog Volumes don't support true POSIX append (raises 'Illegal seek').
    This reads existing content and rewrites the whole file on every emit() —
    slower as the file grows, but avoids append-mode entirely."""
    def __init__(self, path):
        super().__init__()
        self.path = path
        with open(self.path, 'w') as f:
            f.write("")

    def emit(self, record):
        try:
            msg = self.format(record) + "\n"
            try:
                with open(self.path, 'r') as f:
                    existing = f.read()
            except Exception:
                existing = ""
            with open(self.path, 'w') as f:
                f.write(existing + msg)
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            self.handleError(record)


def start_global_notebook_logger(volume_path: str = None):
    """Profiles utilities, DataFrames, and plain variables, matching notebook comments to their
    respective execution lines. Writes directly to a Volume-backed log file so partial logs
    survive a notebook crash. Call this once at the top of your notebook.

    ORDERING: DataFrame/variable diffs are logged automatically once per CELL (after
    everything in that cell has run). Function telemetry (any user-defined function, or
    functions imported from lakebase_utils) always logs in correct real-time order
    regardless, since it's wrapped at the function-object level.

    For DataFrames/variables created mid-cell that you want logged in the EXACT order
    they occur (interleaved correctly with function calls in the same cell), call
    log_checkpoint() manually right after that line — see its docstring below.

    SQL CAPTURE: two mechanisms cover the two ways SQL gets run in a notebook —
      1. spark.sql("...") calls anywhere (inside a function or bare in a cell) are
         caught by patching SparkSession.sql itself, so no code changes are needed
         at call sites. Logged with start/end timestamps and runtime.
      2. %sql / %%sql magic cells are caught separately in the per-cell hook, since
         magic cells never call spark.sql() under the hood. Logged with cell runtime.
    Note: spark.sql() is lazy for DataFrame-returning queries — timing captures query
    planning + immediate execution, not necessarily full downstream materialization
    if the result is acted on later via .collect()/.show()/a write.

    DML RESULT METRICS: for INSERT / UPDATE / DELETE / MERGE / COPY INTO statements
    specifically, the wrapper also collects the small metrics DataFrame Delta returns
    (e.g. num_affected_rows, num_inserted_rows, num_updated_rows, num_deleted_rows) and
    logs it. This does NOT apply to SELECT-style queries — those remain lazy exactly as
    before, since eagerly collecting an arbitrary SELECT result could be expensive and
    would change the query's execution semantics. This only covers spark.sql(...) calls;
    %sql / %%sql magic cells are not covered here (see SQL CAPTURE note above) — their
    output is already shown natively by Databricks' own cell rendering.

    Args:
        volume_path: Optional. Full Unity Catalog Volume path to write logs to
                     (e.g. "/Volumes/dev_audit_log/lakeflow_jobs/privacy_tokenization").
                     Resolution order if not passed directly:
                       1. This function argument
                       2. The "log_volume_path" notebook widget (settable via job params)
                       3. Automatic resolution based on the current secret scope (dev/qa/prod)
    """
    global _current_log_path, _logger_already_initialized

    if _logger_already_initialized:
        print("⚠️ Logger already initialized this session — skipping re-registration.")
        return

    shell = get_ipython()
    dbutils = shell.user_ns.get("dbutils") if shell else None
    if not dbutils:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    notebook_path = ctx.notebookPath().get()
    notebook_name = os.path.basename(notebook_path)

    dbutils.widgets.text("wf_job_name", "")
    workflow_name = dbutils.widgets.get("wf_job_name").strip()

    now = datetime.now()
    date_timestamp = now.strftime("%Y%m%d_%H%M%S")

    if workflow_name:
        workflow_clean = "".join([c if c.isalnum() or c in ('-', '_') else '_' for c in workflow_name])
        file_name = f"{workflow_clean}_{notebook_name}_{date_timestamp}.log"
    else:
        file_name = f"{notebook_name}_{date_timestamp}.log"

    # ============================================================
    # DYNAMIC VOLUME PATH RESOLUTION
    # Priority: function arg > widget > secret-scope-based auto-resolution
    # ============================================================

    dbutils.widgets.text("log_volume_path", "")
    volume_path_widget = dbutils.widgets.get("log_volume_path").strip()

    if volume_path:
        base_path = volume_path.rstrip("/")
    elif volume_path_widget:
        base_path = volume_path_widget.rstrip("/")
    else:
        _SECRET_SCOPE = fetch_secret_scope()
        _env_map = {
            'cgi-eus-dev-ada-kv': 'dev',
            'cgi-eus-qa-ada-kv': 'qa',
            'cgi-eus-prod-ada-kv': 'prod',
        }
        env = _env_map.get(_SECRET_SCOPE)
        if env is None:
            raise ValueError(
                f"Unrecognized secret scope: '{_SECRET_SCOPE}' — cannot resolve log volume path. "
                f"Pass volume_path explicitly or set the 'log_volume_path' widget instead."
            )
        base_path = f"/Volumes/{env}_audit_log/lakeflow_jobs/privacy_tokenization"

    log_path = f"{base_path}/{file_name}"
    _current_log_path = log_path

    logger = logging.getLogger("DeepPipelineProfiler")
    logger.setLevel(logging.INFO)
    logger.handlers = []

    file_handler = VolumeSafeFileHandler(log_path)
    formatter = logging.Formatter('🕒 %(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"📓 Logger initialized for notebook={notebook_name}, workflow={workflow_name or 'manual'}")
    logger.info(f"📁 Log volume path resolved to: {base_path}")

    if not shell:
        return

    for event in ['pre_run_cell', 'post_run_cell', 'post_execute']:
        if event in shell.events.callbacks:
            for cb in list(shell.events.callbacks[event]):
                try: shell.events.unregister(event, cb)
                except Exception: pass

    # ============================================================
    # FUNCTION TELEMETRY (ptk_ / lakebase_ prefixed functions)
    # ============================================================

    class DirectOutputCapture:
        def __init__(self):
            self.buffer = io.StringIO()
            self.original_stdout = sys.stdout
        def __enter__(self):
            sys.stdout = self
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            sys.stdout = self.original_stdout
        def write(self, string):
            self.buffer.write(string)
            self.original_stdout.write(string)
        def flush(self):
            self.original_stdout.flush()
        def get_data(self):
            return self.buffer.getvalue().strip()

    def get_comments_above_line(cell_code, target_line_no):
        lines = cell_code.split('\n')
        matched_comments = []
        idx = target_line_no - 2
        while idx >= 0:
            current_line = lines[idx].strip()
            if current_line.startswith('#'):
                matched_comments.insert(0, current_line)
            elif current_line == '' or current_line.startswith(('import ', 'from ')):
                pass
            else:
                break
            idx -= 1
        return matched_comments

    def make_telemetry_wrapper(original_func, func_name):
        def telemetry_wrapper(*args, **kwargs):
            start_time = datetime.now()
            start_perf = time.perf_counter()

            try:
                frame = inspect.currentframe().f_back
                cell_code = shell.user_ns.get('In', [])[-1]
                line_no = frame.f_lineno
                inline_comments = get_comments_above_line(cell_code, line_no)
                for comment in inline_comments:
                    logger.info(f"💬 Comment context: {comment}")
            except Exception:
                pass

            arg_desc = ""
            if args:
                first_arg = str(args[0]).replace('\n', ' ').strip()
                arg_desc = f" ({first_arg[:60]}...)" if len(first_arg) > 60 else f" ({first_arg})"

            logger.info(f"▶️ [START] {func_name}{arg_desc} at {start_time.strftime('%H:%M:%S.%f')[:-3]}")

            with DirectOutputCapture() as capture:
                try:
                    result = original_func(*args, **kwargs)
                except Exception as e:
                    logger.error(f"❌ [CRASH] {func_name} failed: {str(e)}")
                    raise e

            duration = time.perf_counter() - start_perf
            end_time = datetime.now()
            captured_prints = capture.get_data()

            logger.info(f"⏱️ [END]   {func_name} finished at {end_time.strftime('%H:%M:%S.%f')[:-3]} | Runtime: {duration:.4f}s")
            if captured_prints:
                clean_prints = " | ".join([line.strip() for line in captured_prints.split("\n") if line.strip()])
                logger.info(f"📢 [OUTPUT] {clean_prints}")
            logger.info("-" * 60)
            return result
        return telemetry_wrapper

    def wrap_target_functions():
        """Wraps functions with telemetry. Uses a DENY-LIST approach: any callable is wrapped
        UNLESS it belongs to a known third-party/system library module, OR its name shadows
        a Python builtin (e.g. an internal Databricks helper named 'open' injected into the
        notebook namespace, distinct from the real builtin open()). This avoids relying on
        __globals__ identity checks against shell.user_ns, which was found to be unreliable in
        this Databricks runtime (a plain `def` in a cell did not satisfy that identity check)."""

        _reserved_names = {
            '_log_statement_boundary', 'profiled', 'wrap_target_functions',
            'start_global_notebook_logger', 'finalize_notebook_log', 'log_checkpoint',
        }

        # Module prefixes to SKIP — known libraries/system internals, not your own code
        _excluded_module_prefixes = (
            'pandas', 'pyspark', 'numpy', 'IPython', 'py4j', 'databricks',
            'builtins', 'functools', 'logging', 'importlib',
        )

        # Names matching Python builtins (open, print, len, etc.) — skip regardless of
        # what object currently sits behind the name, since internal tooling sometimes
        # injects plain-function shadows of these into the notebook namespace.
        _builtin_names = set(dir(builtins))

        for var_name, var_value in list(shell.user_ns.items()):
            if var_name in _reserved_names:
                continue
            if var_name in _builtin_names:
                continue
            if var_name.startswith('__') and var_name.endswith('__'):
                continue
            if not callable(var_value):
                continue
            if inspect.isclass(var_value):
                continue
            if inspect.isbuiltin(var_value):
                continue
            if not inspect.isfunction(var_value):
                continue

            func_module = getattr(var_value, '__module__', '') or ''
            is_excluded = func_module.startswith(_excluded_module_prefixes)

            if is_excluded:
                continue

            if not hasattr(var_value, "_is_profiled"):
                wrapped = make_telemetry_wrapper(var_value, var_name)
                wrapped._is_profiled = True
                shell.user_ns[var_name] = wrapped

    # ============================================================
    # SQL QUERY TELEMETRY — patch SparkSession.sql itself
    # ============================================================

    # Statement keywords for which we eagerly collect and log Delta's returned
    # affected-row metrics DataFrame. Deliberately excludes SELECT / WITH / SHOW /
    # DESCRIBE etc., so read-style queries stay exactly as lazy as before.
    _DML_RESULT_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "MERGE", "COPY")

    def _get_statement_keyword(sql_text: str) -> str:
        stripped = (sql_text or "").strip()
        if not stripped:
            return ""
        return stripped.split(None, 1)[0].upper()

    def _log_dml_result_metrics(logger, statement_keyword, result_df):
        """Collects and logs the small metrics DataFrame Delta returns for DML
        statements (e.g. num_affected_rows, num_inserted_rows, num_updated_rows,
        num_deleted_rows). Bounded with .limit() and wrapped in try/except so a
        statement type that doesn't return metrics (or errors on collect) never
        breaks the calling query."""
        try:
            metric_rows = result_df.limit(50).collect()
        except Exception as metrics_exc:
            logger.warning(
                f"⚠️ [SQL RESULT] Could not capture affected-row metrics for "
                f"{statement_keyword}: {metrics_exc}"
            )
            return

        if not metric_rows:
            logger.info(f"📥 [SQL RESULT] {statement_keyword} returned no row-count metrics.")
            return

        for row in metric_rows:
            row_dict = row.asDict()
            metrics_str = ", ".join(f"{k}={v}" for k, v in row_dict.items())
            logger.info(f"📥 [SQL RESULT] {statement_keyword} metrics: {metrics_str}")

    def wrap_spark_sql():
        """Patches SparkSession.sql at the CLASS level so every spark.sql(...) call,
        anywhere in the notebook (inside a wrapped function or bare in a cell), is
        timed and logged automatically — regardless of which variable name holds the
        SparkSession. Idempotent per class: safe to call multiple times.

        Patches BOTH SparkSession implementations, because Databricks Serverless
        and Unity Catalog "shared" access-mode clusters use Spark Connect
        (pyspark.sql.connect.session.SparkSession), which is a DIFFERENT class from
        the classic pyspark.sql.SparkSession — patching only the classic class is a
        silent no-op on those compute types, since the notebook's `spark` object
        never touches the patched class at all.
        """
        classes_to_patch = []

        try:
            from pyspark.sql import SparkSession as _ClassicSparkSession
            classes_to_patch.append(("classic", _ClassicSparkSession))
        except ImportError:
            pass

        try:
            from pyspark.sql.connect.session import SparkSession as _ConnectSparkSession
            classes_to_patch.append(("connect", _ConnectSparkSession))
        except ImportError:
            pass

        patched_classes = []
        for label, _SparkSessionClass in classes_to_patch:
            if hasattr(_SparkSessionClass.sql, "_is_profiled"):
                patched_classes.append(_SparkSessionClass)
                continue

            _original_sql = _SparkSessionClass.sql

            def _profiled_sql(self, sqlQuery, *args, __original=_original_sql, **kwargs):
                start_time = datetime.now()
                start_perf = time.perf_counter()
                query_preview = " ".join(sqlQuery.split())
                query_preview_short = query_preview[:300] + ("..." if len(query_preview) > 300 else "")
                statement_keyword = _get_statement_keyword(sqlQuery)

                logger.info(f"🗄️ [SQL START] {start_time.strftime('%H:%M:%S.%f')[:-3]} | {query_preview_short}")
                try:
                    result = __original(self, sqlQuery, *args, **kwargs)
                except Exception as e:
                    logger.error(f"❌ [SQL CRASH] {str(e)} | Query: {query_preview_short}")
                    raise e

                duration = time.perf_counter() - start_perf
                logger.info(f"⏱️ [SQL END]   Runtime: {duration:.4f}s | {query_preview_short}")

                if statement_keyword in _DML_RESULT_KEYWORDS:
                    _log_dml_result_metrics(logger, statement_keyword, result)

                logger.info("-" * 60)
                return result

            _profiled_sql._is_profiled = True
            _SparkSessionClass.sql = _profiled_sql
            patched_classes.append(_SparkSessionClass)

        # Self-check: does the notebook's actual `spark` object belong to a class
        # we just patched? If not, say so LOUDLY in the log instead of failing
        # silently — this is exactly the failure mode that's hard to debug otherwise.
        notebook_spark = shell.user_ns.get("spark") if shell else None
        if notebook_spark is not None:
            spark_type = type(notebook_spark)
            covered = any(isinstance(notebook_spark, cls) for cls in patched_classes)
            if covered:
                logger.info(f"✅ [SQL PATCH CHECK] Notebook's spark object ({spark_type.__module__}.{spark_type.__name__}) IS covered by the SQL patch.")
            else:
                logger.error(
                    f"⚠️ [SQL PATCH CHECK] Notebook's spark object ({spark_type.__module__}.{spark_type.__name__}) "
                    f"is NOT covered by any patched class ({[c.__name__ for c in patched_classes]}). "
                    f"spark.sql(...) calls will NOT be captured. This class needs to be added to wrap_spark_sql()."
                )
        else:
            logger.info("ℹ️ [SQL PATCH CHECK] Could not find a `spark` object in the notebook namespace to verify against.")

    wrap_spark_sql()

    # ============================================================
    # SHARED TYPE IMPORTS
    # ============================================================

    try:
        import pandas as pd
    except ImportError:
        pd = None
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
    except ImportError:
        SparkDataFrame = None

    df_types = ()
    if pd is not None:
        df_types += (pd.DataFrame,)
    if SparkDataFrame is not None:
        df_types += (SparkDataFrame,)

    # ============================================================
    # RUNNING STATE — persists across cells
    # ============================================================

    _running_df_state = {}
    _running_var_state = set()

    def snapshot_dataframes():
        state = {}
        for var_name, var_value in list(shell.user_ns.items()):
            if var_name.startswith('_'):
                continue
            try:
                if pd is not None and isinstance(var_value, pd.DataFrame):
                    state[var_name] = {
                        "engine": "pandas",
                        "columns": tuple(var_value.columns),
                        "rows": var_value.shape[0],
                    }
                elif SparkDataFrame is not None and isinstance(var_value, SparkDataFrame):
                    entry = {"engine": "spark", "columns": tuple(var_value.columns)}
                    if LOG_SPARK_ROW_COUNTS:
                        try:
                            entry["rows"] = var_value.count()
                        except Exception:
                            entry["rows"] = None
                    state[var_name] = entry
            except Exception:
                continue
        return state

    def snapshot_plain_variables():
        names = set()
        for var_name, var_value in list(shell.user_ns.items()):
            if var_name.startswith('_'):
                continue
            if callable(var_value):
                continue
            if isinstance(var_value, df_types):
                continue
            if inspect.ismodule(var_value):
                continue
            names.add(var_name)
        return names

    def log_dataframe_event(var_name, event, before, after, duration):
        engine = (after or before)["engine"]
        columns = list((after or before)["columns"])
        duration_str = f"{duration:.4f}s" if duration is not None else "N/A"

        if event == "CREATED":
            rows = after.get("rows", "N/A")
            logger.info(
                f"📊 [DATAFRAME CREATED] {var_name} ({engine}) | rows={rows} "
                f"| columns={columns} | cell_duration={duration_str}"
            )
        elif event == "REMOVED":
            logger.info(f"🗑️ [DATAFRAME REMOVED] {var_name} ({engine}) | cell_duration={duration_str}")
        elif event == "MODIFIED":
            before_rows = before.get("rows", "N/A")
            after_rows = after.get("rows", "N/A")
            before_cols = list(before.get("columns", []))
            after_cols = list(after.get("columns", []))
            row_change = f"{before_rows} -> {after_rows}" if before_rows != after_rows else str(after_rows)
            col_change = f"{before_cols} -> {after_cols}" if before_cols != after_cols else str(after_cols)
            logger.info(
                f"📊 [DATAFRAME MODIFIED] {var_name} ({engine}) | rows={row_change} "
                f"| columns={col_change} | cell_duration={duration_str}"
            )

    def diff_dataframes_and_log(duration):
        post_state = snapshot_dataframes()
        all_names = set(_running_df_state.keys()) | set(post_state.keys())
        for var_name in all_names:
            before = _running_df_state.get(var_name)
            after = post_state.get(var_name)
            if before is None and after is not None:
                log_dataframe_event(var_name, "CREATED", before, after, duration)
            elif before is not None and after is None:
                log_dataframe_event(var_name, "REMOVED", before, after, duration)
            elif before is not None and after is not None:
                if before["columns"] != after["columns"] or before.get("rows") != after.get("rows"):
                    log_dataframe_event(var_name, "MODIFIED", before, after, duration)
        _running_df_state.clear()
        _running_df_state.update(post_state)

    def diff_plain_vars_and_log():
        post_vars = snapshot_plain_variables()
        new_vars = post_vars - _running_var_state
        for var_name in new_vars:
            try:
                value = shell.user_ns.get(var_name)
                type_name = type(value).__name__
                logger.info(f"🧩 [VARIABLE CREATED] {var_name} : {type_name}")
            except Exception:
                pass
        _running_var_state.clear()
        _running_var_state.update(post_vars)

    # Initialize running state with whatever already exists (avoids logging pre-existing
    # objects as "newly created" the first time diffing runs)
    _running_df_state.update(snapshot_dataframes())
    _running_var_state.update(snapshot_plain_variables())

    # ============================================================
    # PER-CELL HOOKS (timing + fallback safety net)
    # ============================================================

    _cell_start_perf = {"t": None}
    _diff_done_this_cell = {"flag": False}
    _checkpoint_last_time = {"t": None}

    def _log_statement_boundary(lineno=0):
        """Called by log_checkpoint() to force an immediate diff+log, right where you
        place the call — guarantees DataFrame/variable creation is logged in the exact
        order it happens in your code, rather than waiting for the whole cell to finish."""
        now_perf = time.perf_counter()
        duration = (now_perf - _checkpoint_last_time["t"]) if _checkpoint_last_time["t"] is not None else None
        _checkpoint_last_time["t"] = now_perf
        diff_dataframes_and_log(duration)
        diff_plain_vars_and_log()
        wrap_target_functions()

    # Make the boundary function callable from log_checkpoint() via the notebook namespace
    shell.user_ns['_log_statement_boundary'] = _log_statement_boundary

    def pre_cell_hook(info=None):
        _diff_done_this_cell["flag"] = False
        _cell_start_perf["t"] = time.perf_counter()
        _checkpoint_last_time["t"] = time.perf_counter()

    def post_cell_hook(result=None):
        duration = (time.perf_counter() - _cell_start_perf["t"]) if _cell_start_perf["t"] is not None else None
        try:
            cell_code = shell.user_ns.get('In', [])[-1]
            first_comment_line = next((l.strip() for l in cell_code.split('\n') if l.strip().startswith('#')), None)
            if first_comment_line:
                logger.info(f"💬 Comment context: {first_comment_line}")

            # Detect %sql / %%sql magic cells — these never call spark.sql() directly,
            # so wrap_spark_sql()'s patch can't see them; catch them here instead.
            stripped_code = cell_code.strip()
            if stripped_code.startswith('%sql') or stripped_code.startswith('%%sql'):
                sql_text = (
                    stripped_code.split('\n', 1)[1]
                    if '\n' in stripped_code
                    else stripped_code.replace('%%sql', '', 1).replace('%sql', '', 1).strip()
                )
                sql_preview = " ".join(sql_text.split())[:300]
                duration_str = f"{duration:.4f}s" if duration is not None else "N/A"
                logger.info(f"🗄️ [SQL MAGIC CELL] Runtime: {duration_str} | {sql_preview}")
                logger.info("-" * 60)
        except Exception:
            pass
        diff_dataframes_and_log(duration)
        diff_plain_vars_and_log()
        wrap_target_functions()
        _diff_done_this_cell["flag"] = True

    def post_execute_fallback(*args, **kwargs):
        """Safety net for cells that crash before post_run_cell fires cleanly."""
        if _diff_done_this_cell["flag"]:
            return
        duration = (time.perf_counter() - _cell_start_perf["t"]) if _cell_start_perf["t"] is not None else None
        diff_dataframes_and_log(duration)
        diff_plain_vars_and_log()
        wrap_target_functions()

    wrap_target_functions()
    shell.events.register('pre_run_cell', pre_cell_hook)
    shell.events.register('post_run_cell', post_cell_hook)
    shell.events.register('post_execute', post_execute_fallback)

    _logger_already_initialized = True


def log_checkpoint(label=None, iteration=None, total=None):
    """Call this manually, right after any line that creates/modifies a DataFrame or plain
    variable you want tracked in correct order — or anywhere inside a for/while loop or an
    if/elif/else branch, to mark that point in the log. This is a plain function call
    sitting directly in your code, so it executes exactly where you place it — guaranteed
    by Python itself, independent of cell boundaries or any hook/trace mechanism.

    Usage — DataFrame/variable tracking:
        df = spark.sql("select 1")
        log_checkpoint("df created")

        var1 = 23
        log_checkpoint()                        # label is optional

    Usage — for loop (call once per pass, pass iteration/total for a clean label):
        tables = ["public.tax", "public.temperature"]
        for i, table_name in enumerate(tables, start=1):
            exists = ptk_table_exists(table_name)
            log_checkpoint(f"checked {table_name}", iteration=i, total=len(tables))

    Usage — while loop (call once per pass):
        attempts = 0
        while attempts < max_attempts:
            result = ptk_row_count("public.tax")
            log_checkpoint("retry loop", iteration=attempts)
            attempts += 1

    Usage — if/elif/else branch (call once per branch, describe which branch was taken):
        if row_count > 0:
            log_checkpoint(f"branch: row_count > 0 = True (row_count={row_count})")
            ...
        else:
            log_checkpoint(f"branch: row_count > 0 = False (row_count={row_count})")
            ...
    """
    shell = get_ipython()
    if shell is None:
        return
    boundary_fn = shell.user_ns.get('_log_statement_boundary')
    if boundary_fn is None:
        logging.getLogger("DeepPipelineProfiler").warning(
            "⚠️ log_checkpoint() called but logger not initialized — call start_global_notebook_logger() first."
        )
        return

    if label:
        if iteration is not None:
            iter_str = f" ({iteration}/{total})" if total is not None else f" (iter {iteration})"
        else:
            iter_str = ""
        logging.getLogger("DeepPipelineProfiler").info(f"🔖 Checkpoint{iter_str}: {label}")

    boundary_fn()

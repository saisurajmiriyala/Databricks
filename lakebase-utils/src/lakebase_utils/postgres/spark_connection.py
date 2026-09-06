from __future__ import annotations

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# =========================================================
# GLOBAL SPARK CONFIG HOLDER
# =========================================================
_SPARK_CONN: Optional["PTKSparkConnection"] = None

# =========================================================
# SPARK CONNECTION CLASS
# =========================================================
class PTKSparkConnection:
    def __init__(self, host: str, db: str, user: str, password: str, port: int = 5432):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port

    def get_jdbc_url(self) -> str:
        # Converts standard host strings to Java JDBC format
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"


# =========================================================
# PUBLIC CONNECT FUNCTION
# =========================================================
def ptk_spark_connect(host: str, db: str, user: str, password: str, port: int = 5432) -> None:
    """
    Initializes global connection state for Spark JDBC operations.
    """
    global _SPARK_CONN
    _SPARK_CONN = PTKSparkConnection(host, db, user, password, port)


def _get_spark_config() -> PTKSparkConnection:
    if _SPARK_CONN is None:
        raise Exception("❌ Not connected. Call ptk_spark_connect() first.")
    return _SPARK_CONN

# =========================================================
# INTERNAL HELPER (used by other file)
# =========================================================

def _ptk_get_spark_connection():
    """
    Exposed helper for other modules.
    Returns active connection object.
    """
    return _get_spark_config()

# =========================================================
# DATA UTILITY EXECUTION FUNCTION
# =========================================================
from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
# Assuming you named your global connection holder module path like below:
from lakebase_utils.postgres.spark_connection import _ptk_get_spark_connection

# =========================================================
# PTK SPARK EXECUTE
# =========================================================

def ptk_spark_execute(sql_or_target: str, mode: str = "read", write_df: DataFrame | None = None, write_mode: str = "append") -> DataFrame | int | None:
    """
    Unified Spark Executor.
    - Mode 'read': Handles SELECT/Table targets and returns a distributed Spark DataFrame.
    - Mode 'write': Expects a Spark DataFrame to bulk insert into a table via JDBC.
    """
    conn_obj = _ptk_get_spark_connection()
    spark = SparkSession.builder.getOrCreate()
    
    # Generate the base properties dict for the JDBC connection
    jdbc_url = conn_obj.get_jdbc_url()
    base_options = {
        "url": jdbc_url,
        "user": conn_obj.user,
        "password": conn_obj.password,
        "driver": "org.postgresql.Driver"
    }

    # -------------------------
    # WRITE ACTION
    # -------------------------
    if mode.lower() == "write":
        if write_df is None:
            raise ValueError("❌ 'write_df' parameter must be provided when mode is 'write'.")
            
        print(f"🚀 [Spark] Writing DataFrame to Postgres table '{sql_or_target}' (Mode: {write_mode})...")
        
        # Optimize writing throughput for massive datasets
        (write_df.write
          .format("jdbc")
          .options(**base_options)
          .option("dbtable", sql_or_target)
          .option("batchsize", "50000") 
          .mode(write_mode)
          .save())
          
        print(f"✔ [Spark] WRITE completed successfully to {sql_or_target}.")
        return None

    # -------------------------
    # READ ACTION (SELECT / TABLE)
    # -------------------------
    is_query = "select " in sql_or_target.strip().lower()
    dbtable_target = f"({sql_or_target}) AS ptk_subquery" if is_query else sql_or_target
    base_options["dbtable"] = dbtable_target

    # Inject smart runtime partitioning on the fly to protect clusters from OOM crashes
    try:
        schema_df = spark.read.format("jdbc").options(**base_options).load()
        numeric_types = ("IntegerType", "LongType", "ShortType", "DecimalType", "DoubleType")
        partition_col = next((f.name for f in schema_df.schema if str(f.dataType) in numeric_types), None)
        
        if partition_col:
            safe_col = f"`{partition_col}`"
            bounds = schema_df.select(F.min(safe_col).alias("min"), F.max(safe_col).alias("max")).collect()[0]
            
            if bounds["min"] is not None and bounds["max"] is not None and bounds["min"] != bounds["max"]:
                base_options.update({
                    "partitionColumn": partition_col,
                    "lowerBound": str(bounds["min"]),
                    "upperBound": str(bounds["max"]),
                    "numPartitions": "10"  # Breaks 30M rows into 10 parallel stream slots
                })
    except Exception:
        pass # Graceful fallback to default unpartitioned reading stream

    return spark.read.format("jdbc").options(**base_options).load()


# =========================================================
# PTK SPARK READ DATAFRAME
# =========================================================

def ptk_spark_read_dataframe(sql_or_table: str) -> DataFrame:
    """
    Return query or raw table target directly as a distributed Spark DataFrame.
    """
    return ptk_spark_execute(sql_or_table, mode="read")


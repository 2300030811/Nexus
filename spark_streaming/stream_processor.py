import os
import time

from batch_health import BatchHealth
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from common.logging_utils import get_logger
from common.metrics import SPARK_BATCH_DURATION, SPARK_RECORDS_PROCESSED, start_metrics_server

# ---------------------------------------------------------------------------
# Structured logging (via shared utility)
# ---------------------------------------------------------------------------
logger = get_logger("nexus.spark")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092,localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order_events")
ENV = os.getenv("ENV", "development").strip().lower()


def _env_default_starting_offsets() -> str:
    if ENV in {"development", "local", "test"}:
        return "earliest"
    return "latest"


def _env_default_fail_on_data_loss() -> str:
    if ENV in {"development", "local", "test"}:
        return "false"
    return "true"


KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", _env_default_starting_offsets())
KAFKA_FAIL_ON_DATA_LOSS = os.getenv("KAFKA_FAIL_ON_DATA_LOSS", _env_default_fail_on_data_loss())
KAFKA_MAX_OFFSETS_PER_TRIGGER = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "10000")

from common.db_utils import get_db_config  # noqa: E402

_db_cfg = get_db_config()
PG_HOST = _db_cfg['host']
PG_PORT = _db_cfg['port']
PG_DB = _db_cfg['dbname']
PG_USER = _db_cfg['user']
PG_PASSWORD = _db_cfg['password']

PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "2 minutes"
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/spark-checkpoints")

ORDER_SCHEMA = StructType([
    StructField("event_id",       StringType(), False),
    StructField("event_type",     StringType(), False),
    StructField("timestamp",      StringType(), False),
    StructField("order_id",       StringType(), False),
    StructField("product_id",     StringType(), False),
    StructField("product_name",   StringType(), False),
    StructField("category",       StringType(), False),
    StructField("quantity",       IntegerType(), False),
    StructField("unit_price",     DoubleType(), False),
    StructField("total_amount",   DoubleType(), False),
    StructField("region",         StringType(), False),
    StructField("payment_method", StringType(), False),
])

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Nexus-StreamProcessor")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.streaming.schemaInference", "false")
        # AQE is not supported for structured streaming queries.
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.default.parallelism", "16")
        .config("spark.sql.shuffle.partitions", "16")
        # --- Streaming Robustness ---
        .config("spark.sql.streaming.minBatchesToRetain", "50")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )

def read_kafka_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", KAFKA_FAIL_ON_DATA_LOSS)
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")  # Dropped unused Kafka metadata (key, partition, etc) early
        .select(F.from_json(F.col("json_str"), ORDER_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .drop("timestamp")
    )
    # Deduplication using watermarking (identifies duplicates within the 2m window)
    deduped = (
        parsed.withWatermark("event_timestamp", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    return deduped

# ---------------------------------------------------------------------------
# Shared JDBC Upsert Helper
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Shared JDBC Upsert Helper (Optimized for scale)
# ---------------------------------------------------------------------------
def execute_upsert(batch_df: DataFrame, table_name: str, constraint_cols: list, update_cols: list):
    """
    Execute a native PostgreSQL upsert via psycopg2.
    Optimized: Runs in parallel across Spark executors instead of collecting to driver.
    """
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    columns = batch_df.columns
    col_names = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
    constraint_names = sql.SQL(', ').join(sql.Identifier(col) for col in constraint_cols)
    update_actions = sql.SQL(', ').join(
        sql.Composed([sql.Identifier(c), sql.SQL(" = EXCLUDED."), sql.Identifier(c)])
        for c in update_cols
    )

    def upsert_partition(rows):
        # Create a connection per partition on the executor
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
        try:
            upsert_query = sql.SQL("""
                INSERT INTO {table} ({fields}) VALUES %s
                ON CONFLICT ({pk}) DO UPDATE SET {updates}
            """).format(
                table=sql.Identifier(table_name),
                fields=col_names,
                pk=constraint_names,
                updates=update_actions
            )

            # Batch values into groups of 1000 for efficiency
            partition_data = [tuple(row) for row in rows]
            if not partition_data:
                return

            with conn.cursor() as cur:
                execute_values(cur, upsert_query, partition_data, page_size=1000)
            conn.commit()
        finally:
            conn.close()

    # Scale optimization: Reduce partitions before writing to DB to avoid connection overhead
    # We use 4 partitions as a balanced default for the 6-partition Kafka input.
    batch_df.coalesce(4).foreachPartition(upsert_partition)

# ---------------------------------------------------------------------------
# Sinks with Health Tracking
# ---------------------------------------------------------------------------
_metrics_health  = BatchHealth("revenue_metrics",  failure_threshold=5)
_raw_health      = BatchHealth("raw_events",        failure_threshold=5)
_features_health = BatchHealth("feature_store",     failure_threshold=5)


def write_raw_events_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    start = time.monotonic()
    try:
        execute_upsert(
            batch_df.select(
                "event_id", "event_type", "event_timestamp", "order_id",
                "product_id", "product_name", "category", "quantity",
                "unit_price", "total_amount", "region", "payment_method",
            ),
              "order_events", ["event_id"],
            ["event_type", "event_timestamp", "order_id", "product_id",
             "product_name", "category", "quantity", "unit_price",
             "total_amount", "region", "payment_method"],
        )
        _raw_health.record_success()
        SPARK_RECORDS_PROCESSED.labels(sink="order_events").inc(batch_df.count())
    except Exception as e:
        _raw_health.record_failure(e)
        logger.error("Failed to write raw events batch %d: %s", batch_id, e)
        if not _raw_health.is_healthy:
            raise
    finally:
        SPARK_BATCH_DURATION.labels(sink="order_events").observe(time.monotonic() - start)


def write_metrics_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    start = time.monotonic()
    try:
        execute_upsert(
            batch_df, "revenue_metrics",
            ["window_start", "window_end", "category", "region"],
            ["order_count", "total_revenue", "avg_order_value"],
        )
        _metrics_health.record_success()
        SPARK_RECORDS_PROCESSED.labels(sink="revenue_metrics").inc(batch_df.count())
    except Exception as e:
        _metrics_health.record_failure(e)
        logger.error("Failed to write metrics batch %d: %s", batch_id, e)
        if not _metrics_health.is_healthy:
            raise
    finally:
        SPARK_BATCH_DURATION.labels(sink="revenue_metrics").observe(time.monotonic() - start)


def main() -> None:
    # Start Prometheus metrics endpoint
    metrics_port = int(os.getenv("METRICS_PORT", "9092"))
    start_metrics_server(metrics_port)
    logger.info("Spark metrics available on port %d", metrics_port)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    events = read_kafka_stream(spark)

    # Aggregator
    agg_df = (
        events.groupBy(F.window("event_timestamp", WINDOW_DURATION), "category", "region")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category", "region", "order_count",
            F.round("total_revenue", 2).alias("total_revenue"),
            F.round("avg_order_value", 2).alias("avg_order_value")
        )
    )

    # 1. 5-minute base features
    feature_agg_5m = (
        events.groupBy(
            F.window("event_timestamp", "5 minutes"),
            "category", "region"
        )
        .agg(
            F.round(F.sum("total_amount"), 2).alias("revenue_last_5m"),
            F.count("order_id").alias("orders_last_5m"),
        )
    )

    # 2. 15-minute sliding window features
    feature_agg_15m = (
        events.groupBy(
            F.window("event_timestamp", "15 minutes", "5 minutes"),
            "category", "region"
        )
        .agg(
            F.round(F.sum("total_amount"), 2).alias("revenue_last_15m"),
            F.count("order_id").alias("orders_last_15m"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value_last_15m"),
        )
    )

    # 3. 60-minute sliding window features
    feature_agg_60m = (
        events.groupBy(
            F.window("event_timestamp", "60 minutes", "5 minutes"),
            "category", "region"
        )
        .agg(
            F.round(F.sum("total_amount"), 2).alias("revenue_last_60m"),
            F.count("order_id").alias("orders_last_60m")
        )
    )

    def write_features_5m(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        start = time.monotonic()
        try:
            feats = batch_df.select(
                F.col("window.end").alias("computed_at"),
                "category", "region",
                "revenue_last_5m",
                "orders_last_5m",
            )
            execute_upsert(
                feats, "feature_store",
                ["computed_at", "category", "region"],
                ["revenue_last_5m", "orders_last_5m"]
            )
            _features_health.record_success()
            SPARK_RECORDS_PROCESSED.labels(sink="feature_store_5m").inc(batch_df.count())
        except Exception as e:
            _features_health.record_failure(e)
            logger.error("Failed to write 5m features batch %d: %s", batch_id, e)
        finally:
            SPARK_BATCH_DURATION.labels(sink="feature_store_5m").observe(time.monotonic() - start)

    def write_features_15m(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        start = time.monotonic()
        try:
            feats = batch_df.select(
                F.col("window.end").alias("computed_at"),
                "category", "region",
                "revenue_last_15m",
                "orders_last_15m",
                "avg_order_value_last_15m",
            )
            execute_upsert(
                feats, "feature_store",
                ["computed_at", "category", "region"],
                ["revenue_last_15m", "orders_last_15m", "avg_order_value_last_15m"]
            )
            SPARK_RECORDS_PROCESSED.labels(sink="feature_store_15m").inc(batch_df.count())
        except Exception as e:
            logger.error("Failed to write 15m features batch %d: %s", batch_id, e)
        finally:
            SPARK_BATCH_DURATION.labels(sink="feature_store_15m").observe(time.monotonic() - start)

    def write_features_60m(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        start = time.monotonic()
        try:
            feats = batch_df.select(
                F.col("window.end").alias("computed_at"),
                "category", "region",
                "revenue_last_60m",
                "orders_last_60m",
            )
            execute_upsert(
                feats, "feature_store",
                ["computed_at", "category", "region"],
                ["revenue_last_60m", "orders_last_60m"]
            )
            SPARK_RECORDS_PROCESSED.labels(sink="feature_store_60m").inc(batch_df.count())

            # After writing new features, purge rows older than 2 hours
            # Moved to a more efficient batching approach to minimize DB hits
            # After writing new features, purge rows older than 2 hours
            # Minimal impact check: only run every 10th batch
            if batch_id % 10 == 0:
                import psycopg2
                with psycopg2.connect(
                    host=PG_HOST,
                    port=PG_PORT,
                    dbname=PG_DB,
                    user=PG_USER,
                    password=PG_PASSWORD,
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "DELETE FROM feature_store WHERE computed_at < NOW() - INTERVAL '2 hours'"
                        )
                    conn.commit()

            _features_health.record_success()
        except Exception as e:
            _features_health.record_failure(e)
            logger.error("Failed to write 60m features batch %d: %s", batch_id, e)
        finally:
            SPARK_BATCH_DURATION.labels(sink="feature_store_60m").observe(time.monotonic() - start)

    # Queries
    events.writeStream.foreachBatch(write_raw_events_batch).option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_events").start()
    agg_df.writeStream.foreachBatch(write_metrics_batch).outputMode("update").option("checkpointLocation", f"{CHECKPOINT_DIR}/revenue_metrics").start()
    
    feature_agg_5m.writeStream.foreachBatch(write_features_5m).outputMode("update").option("checkpointLocation", f"{CHECKPOINT_DIR}/feature_store_5m").start()
    feature_agg_15m.writeStream.foreachBatch(write_features_15m).outputMode("update").option("checkpointLocation", f"{CHECKPOINT_DIR}/feature_store_15m").start()
    feature_agg_60m.writeStream.foreachBatch(write_features_60m).outputMode("update").option("checkpointLocation", f"{CHECKPOINT_DIR}/feature_store_60m").start()

    logger.info("Spark streaming started")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown signaled, stopping Spark queries...")
    except Exception as e:
        logger.error("Spark streaming failure: %s", str(e))
        raise

if __name__ == "__main__":
    main()

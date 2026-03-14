import os
import time
import logging
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

from common.logging_utils import get_logger
from batch_health import BatchHealth

# ---------------------------------------------------------------------------
# Structured logging (via shared utility)
# ---------------------------------------------------------------------------
logger = get_logger("nexus.spark")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "order_events")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "nexus")
PG_USER = os.getenv("PG_USER", "nexus")
PG_PASSWORD = os.getenv("PG_PASSWORD", "nexus_password")

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
        # --- AQE & Performance ---
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.default.parallelism", "16")
        .config("spark.sql.shuffle.partitions", "16")
        # --- Streaming Robustness ---
        .config("spark.sql.streaming.maxOffsetsPerTrigger", "10000")
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
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")  # Dropped unused Kafka metadata (key, partition, etc) early
        .select(F.from_json(F.col("json_str"), ORDER_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .drop("timestamp")
    )
    return parsed

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
    from psycopg2.extras import execute_values

    def upsert_partition(rows):
        # Create a connection per partition on the executor
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
        try:
            columns = batch_df.columns
            col_list = ", ".join(columns)
            constraint_str = ", ".join(constraint_cols)
            update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

            upsert_sql = (
                f"INSERT INTO {table_name} ({col_list}) VALUES %s "
                f"ON CONFLICT ({constraint_str}) DO UPDATE SET {update_str}"
            )
            
            # Batch values into groups of 1000 for efficiency
            partition_data = [tuple(row) for row in rows]
            if not partition_data:
                return

            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, partition_data, page_size=1000)
            conn.commit()
        finally:
            conn.close()
        yield True # Return a dummy value to satisfy mapPartitions

    # Execute in parallel on Spark clusters
    batch_df.foreachPartition(upsert_partition)

# ---------------------------------------------------------------------------
# Sinks with Health Tracking
# ---------------------------------------------------------------------------
_metrics_health  = BatchHealth("revenue_metrics",  failure_threshold=5)
_raw_health      = BatchHealth("raw_events",        failure_threshold=5)
_features_health = BatchHealth("feature_store",     failure_threshold=5)


def write_raw_events_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
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
    except Exception as e:
        _raw_health.record_failure(e)
        logger.error("Failed to write raw events batch %d: %s", batch_id, e)
        if not _raw_health.is_healthy:
            raise


def write_metrics_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    try:
        execute_upsert(
            batch_df, "revenue_metrics",
            ["window_start", "window_end", "category", "region"],
            ["order_count", "total_revenue", "avg_order_value"],
        )
        _metrics_health.record_success()
    except Exception as e:
        _metrics_health.record_failure(e)
        logger.error("Failed to write metrics batch %d: %s", batch_id, e)
        # Re-raise so Spark marks the batch as failed and replays from checkpoint
        if not _metrics_health.is_healthy:
            raise

def write_features_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Compute and write feature store with error handling."""
    if batch_df.isEmpty():
        return

    try:
        spark = batch_df.sparkSession
        now_ts = datetime.now(timezone.utc)
        cutoff = (now_ts - timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")
        
        # Use predicate pushdown to filter on the DB side
        try:
            predicate = f"event_timestamp >= '{cutoff}'::timestamptz"
            all_events = spark.read.jdbc(
                url=PG_URL, table="order_events",
                predicates=[predicate], properties=PG_PROPERTIES
            )
        except Exception as read_err:
            logger.warning("Failed to read order_events for feature batch %d: %s", batch_id, read_err)
            return  # Skip this batch
        
        if all_events.isEmpty():
            logger.debug("No recent events for feature batch %d", batch_id)
            return

        spark_now = F.current_timestamp()

        def agg_window(df, minutes, prefix):
            return (
                df.filter(F.col("event_timestamp") >= spark_now - F.expr(f"INTERVAL {minutes} MINUTES"))
                .groupBy("category", "region")
                .agg(
                    F.round(F.sum("total_amount"), 2).alias(f"revenue_last_{prefix}"),
                    F.count("order_id").alias(f"orders_last_{prefix}"),
                    F.round(F.avg("total_amount"), 2).alias(f"avg_order_value_last_{prefix}"),
                )
            )

        w5  = agg_window(all_events, 5,  "5m")
        w15 = agg_window(all_events, 15, "15m")
        w60 = agg_window(all_events, 60, "60m")

        features = (
            w60.join(w15, on=["category", "region"], how="left")
            .join(w5, on=["category", "region"], how="left")
            .fillna(0)
            .withColumn("computed_at", spark_now)
            .withColumn("revenue_trend_pct", 
                        F.when(F.col("revenue_last_15m") > 0, 
                               F.round(F.col("revenue_last_5m") / F.col("revenue_last_15m"), 4))
                        .otherwise(0.0))
            .select(
                "computed_at", "category", "region",
                "revenue_last_5m", "revenue_last_15m", "revenue_last_60m",
                "orders_last_5m", "orders_last_15m", "orders_last_60m",
                "avg_order_value_last_15m",
                "revenue_trend_pct"
            )
        )
        
        # feature_store PK is (computed_at, category, region) but computed_at is always unique
        features.write.jdbc(url=PG_URL, table="feature_store", mode="append", properties=PG_PROPERTIES)
        logger.info("Batch %d: wrote %d feature rows", batch_id, features.count())
    except Exception as e:
        logger.error("Failed to write features batch %d: %s", batch_id, e)
        # Don't re-raise; let the streaming query continue

def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    events = read_kafka_stream(spark)

    # Aggregator
    agg_df = (
        events.withWatermark("event_timestamp", WATERMARK_DELAY)
        .groupBy(F.window("event_timestamp", WINDOW_DURATION), "category", "region")
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

    # Feature Store Aggregation directamente off the streaming events
    feature_agg = (
        events.withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            "category", "region"
        )
        .agg(
            F.round(F.sum("total_amount"), 2).alias("revenue_last_5m"),
            F.count("order_id").alias("orders_last_5m"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value_last_5m"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    def write_features_direct(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty(): return
        try:
            feats = batch_df.select(
                "computed_at", "category", "region",
                "revenue_last_5m", F.lit(0.0).alias("revenue_last_15m"), F.lit(0.0).alias("revenue_last_60m"),
                "orders_last_5m", F.lit(0).alias("orders_last_15m"), F.lit(0).alias("orders_last_60m"),
                F.col("avg_order_value_last_5m").alias("avg_order_value_last_15m"),
                F.lit(0.0).alias("revenue_trend_pct")
            )
            feats.write.jdbc(url=PG_URL, table="feature_store", mode="append", properties=PG_PROPERTIES)
            _features_health.record_success()
        except Exception as e:
            _features_health.record_failure(e)
            logger.error("Failed to write features batch %d: %s", batch_id, e)
            if not _features_health.is_healthy:
                raise

    # Queries
    raw_query = events.writeStream.foreachBatch(write_raw_events_batch).option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_events").start()
    agg_query = agg_df.writeStream.foreachBatch(write_metrics_batch).outputMode("update").option("checkpointLocation", f"{CHECKPOINT_DIR}/revenue_metrics").start()
    feature_query = feature_agg.writeStream.foreachBatch(write_features_direct).option("checkpointLocation", f"{CHECKPOINT_DIR}/feature_store_direct").trigger(processingTime="60 seconds").start()

    logger.info("Spark streaming started")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()

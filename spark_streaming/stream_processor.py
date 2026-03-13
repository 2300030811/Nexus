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
        .config("spark.sql.shuffle.partitions", "8")  # Increased from 4 for better parallelism
        .config("spark.sql.streaming.minBatchesToRetain", "100")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.default.parallelism", "8")
        .config("spark.streaming.kafka.maxRatePerPartition", "1000")  # Rate limiting
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
        raw.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_time")
        .select(F.from_json(F.col("json_str"), ORDER_SCHEMA).alias("data"), "kafka_time")
        .select("data.*", "kafka_time")
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .drop("timestamp")
    )
    return parsed

# ---------------------------------------------------------------------------
# Shared JDBC Upsert Helper
# ---------------------------------------------------------------------------
def execute_upsert(batch_df: DataFrame, table_name: str, constraint_cols: list, update_cols: list):
    """Collect batch to driver and execute a native PostgreSQL upsert via psycopg2."""
    import psycopg2
    from psycopg2.extras import execute_values

    rows = batch_df.collect()
    if not rows:
        return

    columns = batch_df.columns
    col_list = ", ".join(columns)
    constraint_str = ", ".join(constraint_cols)
    update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    upsert_sql = (
        f"INSERT INTO {table_name} ({col_list}) VALUES %s "
        f"ON CONFLICT ({constraint_str}) DO UPDATE SET {update_str}"
    )

    values = [tuple(row) for row in rows]

    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, values)
        conn.commit()
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------
def write_raw_events_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Write raw events with duplicate prevention."""
    if batch_df.isEmpty(): return
    try:
        # Use upsert to handle potential duplicates if checkpoints are reset
        execute_upsert(
            batch_df.select(
                "event_id", "event_type", "event_timestamp", "order_id",
                "product_id", "product_name", "category", "quantity",
                "unit_price", "total_amount", "region", "payment_method",
            ),
            "order_events", ["event_id"], 
            ["event_type", "event_timestamp", "order_id", "product_id", "product_name", 
             "category", "quantity", "unit_price", "total_amount", "region", "payment_method"]
        )
    except Exception as e:
        logger.error("Failed to write raw events batch %d: %s", batch_id, e)

def write_metrics_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Upsert revenue metrics with error handling."""
    if batch_df.isEmpty():
        return
    
    try:
        # Primary Key for revenue_metrics is (window_start, window_end, category, region)
        execute_upsert(
            batch_df, "revenue_metrics", 
            ["window_start", "window_end", "category", "region"],
            ["order_count", "total_revenue", "avg_order_value"]
        )
    except Exception as e:
        logger.error("Failed to write metrics batch %d: %s", batch_id, e)
        # Don't re-raise; let the streaming query continue
        # Checkpoint will ensure we retry next time

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

    # Queries
    raw_query = events.writeStream.foreachBatch(write_raw_events_batch).option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_events").start()
    agg_query = agg_df.writeStream.foreachBatch(write_metrics_batch).outputMode("update").option("checkpointLocation", f"{CHECKPOINT_DIR}/revenue_metrics").start()
    feature_query = events.writeStream.foreachBatch(write_features_batch).option("checkpointLocation", f"{CHECKPOINT_DIR}/feature_store").trigger(processingTime="60 seconds").start()

    logger.info("Spark streaming started")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()

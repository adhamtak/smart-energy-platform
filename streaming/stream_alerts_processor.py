"""
Streaming job: Kafka (ami-readings) -> validate -> dedup -> MinIO + alerts.

Put this file in <project>/streaming/ and run from the project root:
    python streaming/stream_alerts_processor.py

What it does, in order:
  1. Reads readings from Kafka.
  2. Sends broken readings to the dead-letter topic (DLQ).
  3. Removes duplicates.
  4. Writes good readings to MinIO (Bronze, and optionally Silver).
  5. Detects 3 kinds of problems and sends alerts to Kafka (anomaly-alerts).

Rules come from docs/schema.md (the data contract).
"""
import os
import sys
from functools import reduce
from operator import or_
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import (  # noqa: E402
    broadcast, col, concat, concat_ws, current_timestamp, date_format, first,
    from_json, lit, min as _min, sha2, sqrt, struct, substring, sum as _sum,
    to_json, to_timestamp, when, window,
)

from shared.schemas import READING_RAW  # noqa: E402

# ------------------------------------------------------------------
# Settings (all can be overridden from .env)
# ------------------------------------------------------------------
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_READINGS = os.getenv("TOPIC_READINGS", "ami-readings")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "anomaly-alerts")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "ami-readings-dlq")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_USER = os.environ["S3_ROOT_USER"]
S3_PASSWORD = os.environ["S3_ROOT_PASSWORD"]
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
LAKE = f"s3a://{BUCKET}"

# Where the transformer dimension lives. The contract says reference/dim_transformer/,
# but the upload script writes under GEN_S3_PREFIX/runs/<run_id>/reference/...
# Open the MinIO console, find the real folder, and set DIM_TRANSFORMER_PATH in .env.
DIM_TRANSFORMER_PATH = os.getenv(
    "DIM_TRANSFORMER_PATH", f"{LAKE}/reference/dim_transformer/"
)

BRONZE_PATH = os.getenv("BRONZE_PATH", f"{LAKE}/bronze/smart_meter_readings/")
SILVER_PATH = os.getenv("SILVER_PATH", f"{LAKE}/silver/smart_meter_readings/")
WRITE_SILVER = os.getenv("WRITE_SILVER", "false").lower() == "true"  # confirm with team
CHECKPOINTS = f"{LAKE}/checkpoints/streaming_alerts"

# Detection thresholds. These are ASSUMPTIONS: confirm with the team.
VOLTAGE_LOW_V = float(os.getenv("VOLTAGE_LOW_V", "210"))      # nominal is 220 V
VOLTAGE_HIGH_V = float(os.getenv("VOLTAGE_HIGH_V", "250"))
VOLTAGE_PERSIST_INTERVALS = int(os.getenv("VOLTAGE_PERSIST_INTERVALS", "3"))
OVERLOAD_PCT = float(os.getenv("OVERLOAD_PCT", "100"))        # loading % of rated capacity
IRREGULAR_HIGH_KWH = float(os.getenv("IRREGULAR_HIGH_KWH", "5.0"))  # placeholder rule

# How long to wait for late events before closing a time window.
WATERMARK = os.getenv("WATERMARK", "30 minutes")


def build_spark():
    return (
        SparkSession.builder.appName("SmartEnergyStreaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "org.apache.hadoop:hadoop-aws:3.3.2",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_USER)
        .config("spark.hadoop.fs.s3a.secret.key", S3_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def incident(df, *, anomaly_type, asset_type, asset_id, region_id, start_time,
             severity, rule_id, threshold, observed):
    """Shape a stream into the fact_incidents format from the contract.

    incident_id is a hash of type + asset + start_time, so the same incident
    always gets the same id (replays do not create duplicates).
    """
    return df.select(
        concat(
            lit("INC_"),
            substring(
                sha2(concat_ws("|", lit(anomaly_type), asset_id,
                               start_time.cast("string")), 256), 1, 16),
        ).alias("incident_id"),
        lit(anomaly_type).alias("anomaly_type"),
        lit(asset_type).alias("asset_type"),
        asset_id.alias("asset_id"),
        region_id.alias("region_id"),
        current_timestamp().alias("detected_at"),
        start_time.alias("start_time"),
        lit(None).cast("timestamp").alias("end_time"),
        lit("OPEN").alias("status"),
        lit(severity).alias("severity"),
        lit(rule_id).alias("rule_id"),
        lit(float(threshold)).alias("threshold"),
        observed.cast("double").alias("observed_value"),
    )


def send_to_kafka(df, topic, checkpoint_name, mode="append", key=None):
    """Start a query that writes df as JSON messages to a Kafka topic."""
    out = df.select(
        (col(key) if key else lit(None).cast("string")).alias("key"),
        to_json(struct(*df.columns)).alias("value"),
    )
    return (
        out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINTS}/{checkpoint_name}")
        .outputMode(mode)
        .start()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ---------------------------------------------------------------
    # 1. Read from Kafka and parse the JSON using the shared schema
    # ---------------------------------------------------------------
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC_READINGS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    d = col("data")
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS raw_payload")
        .withColumn("data", from_json(col("raw_payload"), READING_RAW))
        .withColumn("event_time", to_timestamp(d["event_timestamp"]))
    )

    # ---------------------------------------------------------------
    # 2. Validation. ONE column decides: error_reason is null = good.
    #    So every record goes to exactly one place (DLQ or Bronze).
    #    Per the contract, only impossible values are invalid. Odd but
    #    possible values (low voltage, high load) are kept: they may be
    #    the anomaly we are looking for.
    # ---------------------------------------------------------------
    required = ["reading_id", "meter_id", "transformer_id", "voltage_v",
                "active_power_kw", "reactive_power_kvar", "energy_kwh",
                "schema_version"]
    missing_any = reduce(or_, [d[c].isNull() for c in required])

    error_reason = (
        when(d.isNull(), lit("malformed JSON"))
        .when(missing_any, lit("missing required field"))
        .when(col("event_time").isNull(), lit("invalid event_timestamp"))
        .when(d["voltage_v"] <= 0, lit("voltage_v must be > 0"))
        .when((d["active_power_kw"] < 0) | (d["energy_kwh"] < 0),
              lit("negative power or energy"))
    )
    parsed = parsed.withColumn("error_reason", error_reason)

    # Bad records -> DLQ (same shape as the contract's dead-letter message)
    dlq = parsed.filter(col("error_reason").isNotNull()).select(
        col("raw_payload"),
        col("error_reason").alias("error"),
        lit(TOPIC_READINGS).alias("source_topic"),
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("failed_at"),
    )
    queries = [send_to_kafka(dlq, TOPIC_DLQ, "dlq")]

    # ---------------------------------------------------------------
    # 3. Good records -> remove duplicates
    #    The generator's reading_id restarts from 1 on every run, so we
    #    dedup on reading_id AND event_time together.
    # ---------------------------------------------------------------
    good = parsed.filter(col("error_reason").isNull()).select("data.*", "event_time")
    deduped = (
        good.withWatermark("event_time", WATERMARK)
        .dropDuplicates(["reading_id", "event_time"])
    )

    # ---------------------------------------------------------------
    # 4. Write to MinIO. Partitioned by day, like the history data.
    #    NOTE: Spark adds a _spark_metadata folder to the output path. If the
    #    history files live in the SAME folder, a batch job reading it may
    #    only see the streamed files. Raise this with the batch owner.
    # ---------------------------------------------------------------
    with_dates = (
        deduped
        .withColumn("year", date_format("event_time", "yyyy"))
        .withColumn("month", date_format("event_time", "MM"))
        .withColumn("day", date_format("event_time", "dd"))
    )

    bronze = with_dates.drop("event_time")  # keeps the original raw columns
    queries.append(
        bronze.writeStream.format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze")
        .partitionBy("year", "month", "day")
        .outputMode("append")
        .start()
    )

    if WRITE_SILVER:
        flag = (
            when(col("voltage_v") < VOLTAGE_LOW_V, lit("LOW_VOLTAGE"))
            .when(col("voltage_v") > VOLTAGE_HIGH_V, lit("HIGH_VOLTAGE"))
        )
        silver = (
            with_dates
            .withColumn("event_timestamp", col("event_time"))
            .withColumn("quality_flag", flag)
            .withColumn("ingested_at", current_timestamp())
            .drop("event_time")
        )
        queries.append(
            silver.writeStream.format("parquet")
            .option("path", SILVER_PATH)
            .option("checkpointLocation", f"{CHECKPOINTS}/silver")
            .partitionBy("year", "month", "day")
            .outputMode("append")
            .start()
        )

    # ---------------------------------------------------------------
    # 5. Detection. First add transformer info (region, rated capacity).
    #    The transformer table is small and does not change, so we read it once.
    # ---------------------------------------------------------------
    dim_tx = spark.read.parquet(DIM_TRANSFORMER_PATH).select(
        "transformer_id", "region_id", "rated_capacity_kva"
    )
    enriched = deduped.join(broadcast(dim_tx), "transformer_id", "left")

    # Rule 1: transformer overload.
    # est_kva = sqrt(total_kw^2 + total_kvar^2), loading = est_kva / rated * 100
    tx = (
        enriched.groupBy(window("event_time", "15 minutes"), "transformer_id")
        .agg(
            _sum("active_power_kw").alias("total_kw"),
            _sum("reactive_power_kvar").alias("total_kvar"),
            first("rated_capacity_kva").alias("rated_kva"),
            first("region_id").alias("region_id"),
        )
        .withColumn("est_kva", sqrt(col("total_kw") * col("total_kw")
                                    + col("total_kvar") * col("total_kvar")))
        .withColumn("loading_pct", col("est_kva") / col("rated_kva") * 100)
        .filter(col("loading_pct") > OVERLOAD_PCT)
    )
    overload = incident(
        tx, anomaly_type="TRANSFORMER_OVERLOAD", asset_type="TRANSFORMER",
        asset_id=col("transformer_id"), region_id=col("region_id"),
        start_time=col("window.start"), severity="CRITICAL",
        rule_id="TX_OVERLOAD_V1", threshold=OVERLOAD_PCT,
        observed=col("loading_pct"),
    )
    # "update" mode: the same incident_id may be sent again while the window
    # fills up. Whoever reads the alerts should upsert by incident_id.
    queries.append(send_to_kafka(overload, TOPIC_ALERTS, "alerts_overload",
                                 mode="update", key="incident_id"))

    # Rule 2: persistent voltage drop = low voltage for N intervals in a row.
    # Sliding window of N*15 minutes that moves every 15 minutes.
    n = VOLTAGE_PERSIST_INTERVALS
    vd = (
        enriched.withColumn("is_low", (col("voltage_v") < VOLTAGE_LOW_V).cast("int"))
        .groupBy(window("event_time", f"{n * 15} minutes", "15 minutes"), "meter_id")
        .agg(
            _sum("is_low").alias("low_count"),
            _min("voltage_v").alias("min_voltage"),
            first("region_id").alias("region_id"),
        )
        .filter(col("low_count") >= n)
    )
    voltage = incident(
        vd, anomaly_type="VOLTAGE_DROP", asset_type="METER",
        asset_id=col("meter_id"), region_id=col("region_id"),
        start_time=col("window.start"), severity="HIGH",
        rule_id="VOLTAGE_DROP_V1", threshold=VOLTAGE_LOW_V,
        observed=col("min_voltage"),
    )
    queries.append(send_to_kafka(voltage, TOPIC_ALERTS, "alerts_voltage",
                                 mode="update", key="incident_id"))

    # Rule 3: irregular consumption.
    # TODO: this is a PLACEHOLDER. The real rule should compare each meter with
    # its own normal usage (meter_baseline, built by the batch job). The
    # generator injects spikes of 2.5-4x and drops to 10-25% of normal, which
    # fixed numbers like these will mostly miss.
    irr = enriched.filter(
        (col("energy_kwh") > IRREGULAR_HIGH_KWH)
        | ((col("active_power_kw") > 0.1) & (col("energy_kwh") == 0.0))
    )
    irregular = incident(
        irr, anomaly_type="IRREGULAR_CONSUMPTION", asset_type="METER",
        asset_id=col("meter_id"), region_id=col("region_id"),
        start_time=col("event_time"), severity="MEDIUM",
        rule_id="IRREGULAR_PLACEHOLDER_V1", threshold=IRREGULAR_HIGH_KWH,
        observed=col("energy_kwh"),
    )
    queries.append(send_to_kafka(irregular, TOPIC_ALERTS, "alerts_irregular",
                                 mode="append", key="incident_id"))

    print(f"Started {len(queries)} streaming queries. Press Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

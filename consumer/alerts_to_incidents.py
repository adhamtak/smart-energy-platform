"""
Consumer: Kafka (anomaly-alerts) -> fact_incidents Parquet on MinIO.

Run from the project root, while the streaming job is producing alerts:
    python consumer/alerts_to_incidents.py

What it does:
  1. Reads every message from the anomaly-alerts topic (from the beginning).
  2. Keeps ONE row per incident_id. The streaming job uses "update" mode for
     overload / voltage alerts, so the same incident_id can arrive several
     times; the latest message wins (upsert).
  3. Every few seconds, rewrites a single Parquet file with all incidents
     (columns and types follow FACT_INCIDENTS in shared/schemas.py).

It does not commit Kafka offsets: each start re-reads the topic from the
beginning and rebuilds the same file, so restarts never lose or duplicate rows.

Settings (all optional, from .env):
    TOPIC_ALERTS            default anomaly-alerts
    KAFKA_BOOTSTRAP_SERVERS default localhost:9092
    INCIDENTS_PREFIX        default gold/fact_incidents   (confirm with the team)
    FLUSH_SECONDS           default 10
"""
import io
import json
import os
import signal
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

TOPIC = os.getenv("TOPIC_ALERTS", "anomaly-alerts")
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
PREFIX = os.getenv("INCIDENTS_PREFIX", "gold/fact_incidents").strip("/")
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "10"))
KEY = f"{PREFIX}/part-00000.parquet"

# Same columns, same order as FACT_INCIDENTS in shared/schemas.py.
COLUMNS = [
    "incident_id", "anomaly_type", "asset_type", "asset_id", "region_id",
    "detected_at", "start_time", "end_time", "status", "severity",
    "rule_id", "threshold", "observed_value",
]
TIME_COLUMNS = ["detected_at", "start_time", "end_time"]
FLOAT_COLUMNS = ["threshold", "observed_value"]

STOP_REQUESTED = False


def request_stop(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


def upsert(incidents, message):
    """Store one alert in the dict. Returns True if it was usable."""
    if not isinstance(message, dict) or not message.get("incident_id"):
        return False
    incidents[message["incident_id"]] = message
    return True


def to_frame(incidents):
    """Turn the dict of alerts into a DataFrame with the contract's types."""
    frame = pd.DataFrame(list(incidents.values()))
    # to_json leaves out null fields (end_time), so add any missing column.
    frame = frame.reindex(columns=COLUMNS)

    for name in TIME_COLUMNS:
        frame[name] = pd.to_datetime(frame[name], utc=True, errors="coerce")
    for name in FLOAT_COLUMNS:
        frame[name] = pd.to_numeric(frame[name], errors="coerce").astype("float64")

    return frame.sort_values("incident_id").reset_index(drop=True)


def to_parquet_bytes(frame):
    """Serialise to Parquet with microsecond timestamps.

    Spark 3.3 cannot read nanosecond timestamps, and pandas uses nanoseconds
    by default, so we force microseconds here.
    """
    buffer = io.BytesIO()
    pq.write_table(
        pa.Table.from_pandas(frame, preserve_index=False),
        buffer,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    return buffer.getvalue()


def main():
    import boto3
    from botocore.config import Config
    from kafka import KafkaConsumer

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv(
            "S3_ENDPOINT_URL",
            f"http://localhost:{os.getenv('S3_API_PORT', '9000')}",
        ),
        aws_access_key_id=os.getenv("S3_ROOT_USER"),
        aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    s3.head_bucket(Bucket=BUCKET)  # fail fast if MinIO / bucket is unavailable

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA,
        group_id=None,               # no stored offsets: always rebuild from the start
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )

    incidents = {}
    skipped = 0
    dirty = False
    last_flush = 0.0

    def flush():
        nonlocal dirty, last_flush
        if incidents:
            s3.put_object(
                Bucket=BUCKET, Key=KEY,
                Body=to_parquet_bytes(to_frame(incidents)),
            )
            print(f"Wrote {len(incidents):,} incidents -> s3://{BUCKET}/{KEY}")
        dirty = False
        last_flush = time.monotonic()

    print(f"Reading {TOPIC} from {KAFKA}. Press Ctrl+C to stop.")

    while not STOP_REQUESTED:
        for record in consumer.poll(timeout_ms=1000, max_records=5000).values():
            for item in record:
                try:
                    message = json.loads(item.value.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    skipped += 1
                    continue
                if upsert(incidents, message):
                    dirty = True
                else:
                    skipped += 1

        if dirty and time.monotonic() - last_flush >= FLUSH_SECONDS:
            flush()

    if dirty:
        flush()
    consumer.close()
    print(f"Stopped. {len(incidents):,} incidents, {skipped} skipped messages.")


if __name__ == "__main__":
    sys.exit(main())

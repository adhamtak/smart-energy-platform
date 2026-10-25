"""Kafka -> MinIO (Bronze) ingestion for the streaming readings.

Layout (same Hive-style date partitions as the historical upload):

    s3://<bucket>/bronze/streaming/readings/year=YYYY/month=MM/day=DD/part-<id>.parquet

* Files are partitioned by the reading's own event date, never by wall clock;
  a batch that spans midnight is split into one file per day.
* Parquet uses the historical schema exactly (string timestamps, int32
  schema_version), so history and stream can be unioned without casts.
* Offsets are committed only after the data is in MinIO (at-least-once). The
  file name is derived from the offset ranges, so a replayed batch overwrites
  itself; any remaining duplicates are removed on reading_id in Silver.
* When the producer's DAY_CLOSED control message has arrived on every
  partition, the consumer writes <day>/_DAY_CLOSED. Airflow waits on that.
  Run ONE consumer instance per topic.
"""

import hashlib
import io
import json
import os
import signal
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv


def find_project_root(start: Path) -> Path:
    start = Path(start).resolve()
    for candidate in (start, *start.parents):
        if any((candidate / marker).exists()
               for marker in (".env", "docker-compose.yml", ".git")):
            return candidate
    return start


PROJECT_ROOT = find_project_root(Path(__file__).parent)
load_dotenv(PROJECT_ROOT / ".env")

TOPIC = os.getenv("KAFKA_INGEST_TOPIC", "ami-readings-stream")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "minio-ingestion-bronze")
BUCKET = os.getenv("S3_BUCKET", "smart-energy")
PREFIX = os.getenv("STREAM_S3_PREFIX", "bronze/streaming/readings").strip("/")
REJECTED_PREFIX = os.getenv(
    "STREAM_S3_REJECTED_PREFIX", "bronze/streaming/_rejected"
).strip("/")
BATCH_SIZE = int(os.getenv("STREAM_BATCH_SIZE", "50000"))
FLUSH_SECONDS = float(os.getenv("STREAM_FLUSH_SECONDS", "30"))

# Must match the historical readings parquet schema (large_string / int32).
READING_SCHEMA = pa.schema([
    ("reading_id", pa.large_string()),
    ("event_timestamp", pa.large_string()),
    ("meter_id", pa.large_string()),
    ("transformer_id", pa.large_string()),
    ("voltage_v", pa.float64()),
    ("active_power_kw", pa.float64()),
    ("reactive_power_kvar", pa.float64()),
    ("energy_kwh", pa.float64()),
    ("power_factor", pa.float64()),
    ("schema_version", pa.int32()),
])

STOP_REQUESTED = False


def request_stop(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


def make_s3_client():
    endpoint = os.getenv(
        "S3_ENDPOINT_URL",
        f"http://localhost:{os.getenv('S3_API_PORT', '9000')}",
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("S3_ROOT_USER"),
        aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def day_prefix(base: str, day: str) -> str:
    """'2026-07-07' -> '<base>/year=2026/month=07/day=07'."""
    year, month, dd = day.split("-")
    return f"{base}/year={year}/month={month}/day={dd}"


def validate_timestamp(value) -> str:
    """Require the historical format YYYY-MM-DDTHH:MM:SSZ and return the day."""
    if not isinstance(value, str) or len(value) != 20 or not value.endswith("Z"):
        raise ValueError(f"Bad event_timestamp: {value!r}")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise ValueError(f"Bad event_timestamp: {value!r}") from exc
    return value[:10]


def build_table(records: list) -> pa.Table:
    table = pa.Table.from_pylist(records, schema=READING_SCHEMA)
    for field in READING_SCHEMA:
        if table.column(field.name).null_count:
            raise ValueError(f"Null values in column {field.name!r}.")
    return table


class BronzeIngestor:
    """Buffers readings, writes date-partitioned Parquet, commits offsets."""

    def __init__(self, s3, bucket, prefix, commit_fn,
                 batch_size=BATCH_SIZE, flush_seconds=FLUSH_SECONDS):
        self.s3 = s3
        self.bucket = bucket
        self.prefix = prefix
        self.commit_fn = commit_fn
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds

        self.buffer = []
        self.rejected = []
        self.pending = {}   # (topic, partition) -> next offset to commit
        self.ranges = {}    # (topic, partition) -> [first, last] offsets
        self.received = 0
        self.uploaded = 0
        self.rejected_total = 0
        self.days_closed = []
        self.last_flush = time.monotonic()

    # -- message handling ------------------------------------------------

    def handle(self, topic, partition, offset, value):
        try:
            if isinstance(value, dict) and value.get("type") == "DAY_CLOSED":
                self._handle_day_closed(topic, partition, offset, value)
                return
            self._add_reading(value)
        except (ValueError, KeyError, TypeError) as exc:
            self.rejected.append({
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "error": str(exc),
                "value": value,
            })
            self.rejected_total += 1
            print(f"Rejected message {topic}[{partition}]@{offset}: {exc}")

        self._note_offset(topic, partition, offset)

        if len(self.buffer) >= self.batch_size:
            self.flush()

    def _add_reading(self, value):
        if not isinstance(value, dict):
            raise ValueError("Kafka message is not a JSON object.")
        if not value.get("reading_id") or not value.get("meter_id"):
            raise ValueError("Message is missing reading_id or meter_id.")
        validate_timestamp(value.get("event_timestamp"))
        self.buffer.append(value)
        self.received += 1

    def _note_offset(self, topic, partition, offset):
        key = (topic, partition)
        self.pending[key] = offset + 1
        span = self.ranges.setdefault(key, [offset, offset])
        span[1] = offset

    # -- day close -------------------------------------------------------

    def _handle_day_closed(self, topic, partition, offset, message):
        day = message["dt"]
        validate_timestamp(f"{day}T00:00:00Z")
        partitions = int(message["partitions"])
        expected_rows = int(message["expected_rows"])

        # Everything consumed so far (including this partition's data for
        # the day) must be in MinIO before the marker is written.
        self._upload_buffer()

        base = day_prefix(self.prefix, day)
        self._put_json(f"{base}/_DAY_CLOSED.p{partition}", {
            "dt": day,
            "partition": partition,
            "expected_rows": expected_rows,
            "closed_at": datetime.now(timezone.utc).isoformat(),
        })

        listing = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=f"{base}/_DAY_CLOSED.p"
        )
        closed_partitions = listing.get("KeyCount", 0)

        if closed_partitions >= partitions:
            self._put_json(f"{base}/_DAY_CLOSED", {
                "dt": day,
                "expected_rows": expected_rows,
                "partitions": partitions,
                "closed_at": datetime.now(timezone.utc).isoformat(),
            })
            self.days_closed.append(day)
            print(f"DAY CLOSED: {day} -> s3://{self.bucket}/{base}/_DAY_CLOSED")
        else:
            print(f"Day {day}: {closed_partitions}/{partitions} partitions closed")

        self._note_offset(topic, partition, offset)
        self._commit()

    # -- flushing --------------------------------------------------------

    def maybe_flush(self):
        if time.monotonic() - self.last_flush >= self.flush_seconds:
            if self.buffer or self.pending or self.rejected:
                self.flush()
            else:
                self.last_flush = time.monotonic()

    def flush(self):
        """Upload buffered rows, then commit their offsets."""
        self._upload_buffer()
        self._commit()
        self.last_flush = time.monotonic()

    def _batch_id(self):
        material = json.dumps(
            sorted((t, p, span) for (t, p), span in self.ranges.items())
        )
        return hashlib.sha1(material.encode()).hexdigest()[:16]

    def _upload_buffer(self):
        if not self.buffer and not self.rejected:
            return

        batch_id = self._batch_id() if self.ranges else uuid.uuid4().hex[:16]

        by_day = defaultdict(list)
        for record in self.buffer:
            by_day[record["event_timestamp"][:10]].append(record)

        for day, records in sorted(by_day.items()):
            table = build_table(records)
            body = io.BytesIO()
            pq.write_table(table, body, compression="snappy")
            key = f"{day_prefix(self.prefix, day)}/part-{batch_id}.parquet"
            self.s3.put_object(
                Bucket=self.bucket, Key=key, Body=body.getvalue()
            )
            print(f"Uploaded {len(records):,} rows -> s3://{self.bucket}/{key}")

        self.uploaded += len(self.buffer)
        self.buffer.clear()

        if self.rejected:
            payload = "\n".join(
                json.dumps(item, default=str) for item in self.rejected
            )
            self.s3.put_object(
                Bucket=self.bucket,
                Key=f"{REJECTED_PREFIX}/part-{batch_id}.jsonl",
                Body=payload.encode("utf-8"),
            )
            self.rejected.clear()

    def _commit(self):
        if self.pending:
            self.commit_fn(dict(self.pending))
            self.pending.clear()
        self.ranges.clear()

    def _put_json(self, key, payload):
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload).encode("utf-8"),
        )


def make_offset(offset):
    """OffsetAndMetadata across kafka-python versions."""
    from kafka.structs import OffsetAndMetadata

    try:
        return OffsetAndMetadata(offset, "", -1)
    except TypeError:
        return OffsetAndMetadata(offset, "")


def main():
    global STOP_REQUESTED

    from kafka import KafkaConsumer, TopicPartition

    s3 = make_s3_client()
    # Fail immediately if MinIO or the target bucket is unavailable.
    s3.head_bucket(Bucket=BUCKET)

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=5000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    consumer.subscribe([TOPIC])

    def commit(offsets):
        consumer.commit({
            TopicPartition(topic, partition): make_offset(offset)
            for (topic, partition), offset in offsets.items()
        })

    ingestor = BronzeIngestor(s3, BUCKET, PREFIX, commit)

    previous_sigint = signal.signal(signal.SIGINT, request_stop)
    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)

    start = time.monotonic()
    last_status = start

    print(f"Topic: {TOPIC} (group {GROUP_ID})")
    print(f"Destination: s3://{BUCKET}/{PREFIX}/year=YYYY/month=MM/day=DD/")
    print(f"Batch size: {BATCH_SIZE:,} rows or {FLUSH_SECONDS:.0f}s")
    print("Consumer running. Press Ctrl+C to stop.")

    try:
        while not STOP_REQUESTED:
            records = consumer.poll(timeout_ms=1000, max_records=5000)

            for messages in records.values():
                for message in messages:
                    ingestor.handle(
                        message.topic,
                        message.partition,
                        message.offset,
                        message.value,
                    )

            ingestor.maybe_flush()

            now = time.monotonic()
            if now - last_status >= 10:
                print(
                    f"Received: {ingestor.received:,} | "
                    f"Uploaded: {ingestor.uploaded:,} | "
                    f"Buffered: {len(ingestor.buffer):,} | "
                    f"Rate: {ingestor.received / max(now - start, 0.001):.1f}/s"
                )
                last_status = now

    except KeyboardInterrupt:
        STOP_REQUESTED = True

    finally:
        try:
            ingestor.flush()
        finally:
            consumer.close()
            signal.signal(signal.SIGINT, previous_sigint)
            signal.signal(signal.SIGTERM, previous_sigterm)

        elapsed = max(time.monotonic() - start, 0.001)
        print("\nConsumer stopped.")
        print(f"Messages received: {ingestor.received:,}")
        print(f"Rows uploaded to MinIO: {ingestor.uploaded:,}")
        print(f"Rejected messages: {ingestor.rejected_total:,}")
        print(f"Days closed: {', '.join(ingestor.days_closed) or 'none'}")
        print(f"Elapsed time: {elapsed:.1f} seconds")


if __name__ == "__main__":
    main()
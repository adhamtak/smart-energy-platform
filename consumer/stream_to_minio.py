
import json
import os
import time
import uuid
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

TOPIC = "ami-readings-stream"
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
PREFIX = "bronze/streaming"
BATCH_SIZE = 5_000
TARGET_MESSAGES = 100_000


def main():
    endpoint = f"http://localhost:{os.getenv('S3_API_PORT', '9000')}"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("S3_ROOT_USER"),
        aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
        region_name="us-east-1",
    )

    # Verify that the target bucket is accessible.
    s3.head_bucket(Bucket=BUCKET)

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    consumer.subscribe([TOPIC])

    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    batch = []
    received = 0
    uploaded = 0

    print(f"Topic: {TOPIC}")
    print(f"Target: {TARGET_MESSAGES:,} messages")
    print(f"Destination: s3://{BUCKET}/{PREFIX}/")
    print("Waiting for Kafka assignment...")

    try:
        # Poll until partitions are assigned.
        deadline = time.monotonic() + 30
        while not consumer.assignment():
            consumer.poll(timeout_ms=1000)
            if time.monotonic() > deadline:
                raise TimeoutError("Kafka partitions were not assigned.")

        # This is a dedicated demo topic. Start at its current earliest
        # available offsets and process up to the expected demo count.
        last_progress = time.monotonic()
        while received < TARGET_MESSAGES:
            records = consumer.poll(timeout_ms=1000, max_records=1000)

        if records:
            last_progress = time.monotonic()
        elif time.monotonic() - last_progress > 15:
            raise TimeoutError(
                f"No Kafka messages received for 15 seconds. "
                f"Received {received:,}/{TARGET_MESSAGES:,}."
            )

            for messages in records.values():
                for message in messages:
                    record = message.value

                    if not isinstance(record, dict):
                        raise ValueError("Kafka message is not a JSON object.")

                    if not record.get("reading_id") or not record.get("meter_id"):
                        raise ValueError("Message is missing reading_id or meter_id.")

                    batch.append(record)
                    received += 1

                    if len(batch) >= BATCH_SIZE:
                        uploaded += upload_batch(
                            s3, batch, run_id, uploaded // BATCH_SIZE
                        )
                        batch.clear()
                        print(
                            f"Received {received:,}/{TARGET_MESSAGES:,}; "
                            f"uploaded {uploaded:,}"
                        )

                    if received >= TARGET_MESSAGES:
                        break

                if received >= TARGET_MESSAGES:
                    break

        if batch:
            uploaded += upload_batch(
                s3, batch, run_id, uploaded // BATCH_SIZE
            )
            batch.clear()

        print("\nIngestion completed.")
        print(f"Messages received: {received:,}")
        print(f"Messages uploaded: {uploaded:,}")
        print(f"Destination: s3://{BUCKET}/{PREFIX}/run_id={run_id}/")

    finally:
        consumer.close()


def upload_batch(s3, records, run_id, batch_number):
    frame = pd.DataFrame(records)

    # Use UTC-aware timestamps where possible.
    if "event_timestamp" in frame.columns:
        frame["event_timestamp"] = pd.to_datetime(
            frame["event_timestamp"], utc=True, errors="raise"
        )

    local_path = PROJECT_ROOT / "data" / f"stream_batch_{uuid.uuid4().hex}.parquet"
    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        frame.to_parquet(local_path, index=False, engine="pyarrow")

        key = (
            f"{PREFIX}/run_id={run_id}/"
            f"batch-{batch_number:05d}-{uuid.uuid4().hex}.parquet"
        )

        s3.upload_file(str(local_path), BUCKET, key)
        print(f"Uploaded {len(records):,} rows -> s3://{BUCKET}/{key}")
        return len(records)

    finally:
        local_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()

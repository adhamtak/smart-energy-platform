
import json
import os
import signal
import time
import uuid
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

TOPIC = os.getenv("KAFKA_INGEST_TOPIC", "ami-readings-dev")
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
PREFIX = "bronze/streaming"
BATCH_SIZE = 5_000

STOP_REQUESTED = False


def request_stop(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


def upload_batch(s3, records, run_id, batch_number):
    """Write one batch to Parquet and upload it to MinIO."""
    frame = pd.DataFrame(records)

    if "event_timestamp" in frame.columns:
        frame["event_timestamp"] = pd.to_datetime(
            frame["event_timestamp"], utc=True, errors="raise"
        )

    local_dir = PROJECT_ROOT / "data" / "stream_batches"
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / f"batch_{uuid.uuid4().hex}.parquet"

    key = (
        f"{PREFIX}/run_id={run_id}/"
        f"batch-{batch_number:06d}.parquet"
    )

    try:
        frame.to_parquet(local_path, index=False, engine="pyarrow")

        # upload_file returns only after the object has been uploaded.
        s3.upload_file(str(local_path), BUCKET, key)

        print(
            f"Uploaded batch {batch_number}: {len(records):,} rows "
            f"-> s3://{BUCKET}/{key}"
        )
        return len(records)

    finally:
        local_path.unlink(missing_ok=True)


def main():
    global STOP_REQUESTED

    endpoint = f"http://localhost:{os.getenv('S3_API_PORT', '9000')}"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("S3_ROOT_USER"),
        aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
        region_name="us-east-1",
    )

    # Fail immediately if MinIO or the target bucket is unavailable.
    s3.head_bucket(Bucket=BUCKET)

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        group_id=f"minio-ingestion-{uuid.uuid4().hex[:8]}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    consumer.subscribe([TOPIC])

    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    batch = []
    received = 0
    uploaded = 0
    batch_number = 0
    start = time.monotonic()
    last_upload = start

    previous_sigint = signal.signal(signal.SIGINT, request_stop)
    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)

    print(f"Topic: {TOPIC}")
    print(f"Destination: s3://{BUCKET}/{PREFIX}/run_id={run_id}/")
    print(f"Batch size: {BATCH_SIZE:,}")
    print("Consumer running. Press Ctrl+C to stop.")
    print("Waiting for Kafka partition assignment...")

    try:
        deadline = time.monotonic() + 30

        while not consumer.assignment() and not STOP_REQUESTED:
            consumer.poll(timeout_ms=1000, max_records=1000)
            if time.monotonic() > deadline:
                raise TimeoutError(
                    "Kafka partitions were not assigned within 30 seconds."
                )

        if STOP_REQUESTED:
            return

        print(f"Assigned partitions: {consumer.assignment()}")

        while not STOP_REQUESTED:
            records = consumer.poll(
                timeout_ms=1000,
                max_records=1000,
            )

            for messages in records.values():
                for message in messages:
                    record = message.value

                    if not isinstance(record, dict):
                        raise ValueError(
                            "Kafka message is not a JSON object."
                        )

                    if not record.get("reading_id") or not record.get("meter_id"):
                        raise ValueError(
                            "Message is missing reading_id or meter_id."
                        )

                    batch.append(record)
                    received += 1

                    if len(batch) >= BATCH_SIZE:
                        batch_number += 1
                        uploaded += upload_batch(
                            s3, batch, run_id, batch_number
                        )
                        batch.clear()
                        last_upload = time.monotonic()

            # Periodic status even if the stream is quiet.
            now = time.monotonic()
            if now - last_upload >= 10:
                elapsed = max(now - start, 0.001)
                print(
                    f"Received: {received:,} | "
                    f"Uploaded: {uploaded:,} | "
                    f"Average received rate: {received / elapsed:.1f}/s"
                )
                last_upload = now

    except KeyboardInterrupt:
        # Handles Ctrl+C if it occurs outside the signal handler.
        STOP_REQUESTED = True

    finally:
        try:
            if batch:
                batch_number += 1
                uploaded += upload_batch(
                    s3, batch, run_id, batch_number
                )
                batch.clear()
        finally:
            consumer.close()
            signal.signal(signal.SIGINT, previous_sigint)
            signal.signal(signal.SIGTERM, previous_sigterm)

        elapsed = max(time.monotonic() - start, 0.001)

        print("\nConsumer stopped.")
        print(f"Messages received: {received:,}")
        print(f"Rows uploaded to MinIO: {uploaded:,}")
        print(f"Elapsed time: {elapsed:.1f} seconds")
        print(f"Destination: s3://{BUCKET}/{PREFIX}/run_id={run_id}/")

        if received == uploaded:
            print("COUNT CHECK PASSED: received rows match uploaded rows.")
        else:
            print("WARNING: received and uploaded counts differ.")


if __name__ == "__main__":
    main()

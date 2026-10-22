
import argparse
import json
import os
import signal
import time
import uuid
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "generated-100k-1day"

REQUIRED_COLUMNS = {
    "reading_id",
    "event_timestamp",
    "meter_id",
    "transformer_id",
    "voltage_v",
    "active_power_kw",
    "reactive_power_kvar",
    "energy_kwh",
    "power_factor",
    "schema_version",
}

STOP_REQUESTED = False


def request_stop(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


def json_default(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def load_interval(data_dir, timestamp):
    """Load and validate one interval containing one reading per meter."""
    readings_dir = data_dir / "readings"
    files = sorted(readings_dir.glob("part-*.parquet"))

    if not files:
        raise FileNotFoundError(f"No Parquet files found in {readings_dir}")

    target = pd.to_datetime(timestamp, utc=True)
    frames = []

    print(f"Scanning {len(files):,} Parquet files for {target}...")

    for index, path in enumerate(files, start=1):
        frame = pd.read_parquet(
            path,
            columns=sorted(REQUIRED_COLUMNS),
        )

        missing = REQUIRED_COLUMNS - set(frame.columns)
        if missing:
            raise ValueError(
                f"{path.name} is missing columns: {sorted(missing)}"
            )

        frame["event_timestamp"] = pd.to_datetime(
            frame["event_timestamp"], utc=True
        )
        frame = frame.loc[frame["event_timestamp"] == target]

        if not frame.empty:
            frames.append(frame)

        if index % 50 == 0 or index == len(files):
            print(f"Scanned {index:,}/{len(files):,} files")

    if not frames:
        raise ValueError(f"No readings found for {target}")

    df = pd.concat(frames, ignore_index=True)

    if df["meter_id"].duplicated().any():
        raise ValueError("Duplicate meter readings found in this interval.")

    df = df.sort_values(
        ["transformer_id", "meter_id"]
    ).reset_index(drop=True)

    df = df.astype(object).where(pd.notna(df), None)

    return df, len(files), target


def make_interval(base_df, timestamp, interval_number):
    """
    Create a distinct synthetic interval from a validated template.

    Measurement values are reused as synthetic test values.
    This does not represent newly measured electricity consumption.
    """
    df = base_df.copy()

    df["event_timestamp"] = pd.to_datetime(timestamp, utc=True)

    # Deterministic IDs: unique for each simulated interval and meter.
    df["reading_id"] = [
        f"sim-{interval_number:06d}-{meter_id}"
        for meter_id in df["meter_id"]
    ]

    return df.astype(object).where(pd.notna(df), None)


def main():
    parser = argparse.ArgumentParser(
        description="Continuously publish synthetic AMI readings to Kafka."
    )

    parser.add_argument("--input-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--topic", default="ami-readings-stream")
    parser.add_argument(
        "--interval-timestamp",
        default="2026-07-01T00:00:00Z",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=300,
        help="Target publishing rate in events per second.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Continue publishing until Ctrl+C or a configured limit.",
    )
    parser.add_argument(
        "--duration-minutes",
        type=float,
        default=None,
        help="Optional wall-clock duration limit.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Optional maximum number of events to publish.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Actually send messages. Without this flag, preview only.",
    )

    args = parser.parse_args()

    if args.rate <= 0:
        parser.error("--rate must be greater than zero.")

    if args.duration_minutes is not None:
        if args.duration_minutes <= 0:
            parser.error("--duration-minutes must be positive.")
        if not args.continuous:
            parser.error("--duration-minutes requires --continuous.")

    if args.max_events is not None:
        if args.max_events <= 0:
            parser.error("--max-events must be positive.")
        if not args.continuous:
            parser.error("--max-events requires --continuous.")

    data_dir = args.input_dir.resolve()
    manifest_path = data_dir / "manifest.json"

    if not manifest_path.is_file():
        raise FileNotFoundError(f"manifest.json not found in {data_dir}")

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    if manifest.get("data_type") != "synthetic":
        raise ValueError("Expected a dataset marked as synthetic.")

    base_df, file_count, first_timestamp = load_interval(
        data_dir, args.interval_timestamp
    )

    unique_meters = base_df["meter_id"].nunique()

    print(f"\nDataset: {data_dir}")
    print(f"Parquet files scanned: {file_count:,}")
    print(f"First interval: {first_timestamp}")
    print(f"Readings per interval: {len(base_df):,}")
    print(f"Unique meters: {unique_meters:,}")
    print(f"Topic: {args.topic}")
    print(f"Target rate: {args.rate:,.0f} events/second")

    if len(base_df) != 100_000 or unique_meters != 100_000:
        raise ValueError(
            "Expected 100,000 readings from 100,000 unique meters."
        )

    if not args.publish:
        print("\nDRY RUN: no messages will be sent.")
        print(
            base_df[
                ["reading_id", "event_timestamp", "meter_id"]
            ].head().to_string(index=False)
        )
        print("\nValidation passed. Add --publish to send messages.")
        return

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        acks="all",
        retries=5,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            default=json_default,
            allow_nan=False,
        ).encode("utf-8"),
    )

    global STOP_REQUESTED
    STOP_REQUESTED = False

    previous_sigint = signal.signal(signal.SIGINT, request_stop)
    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)

    start = time.monotonic()
    total_sent = 0
    interval_number = 0
    current_timestamp = first_timestamp

    duration_limit = (
        args.duration_minutes * 60
        if args.duration_minutes is not None
        else None
    )

    try:
        print("\nPublishing started. Press Ctrl+C to stop.")

        while not STOP_REQUESTED:
            if duration_limit is not None:
                if time.monotonic() - start >= duration_limit:
                    break

            if args.max_events is not None:
                if total_sent >= args.max_events:
                    break

            interval_number += 1

            if interval_number == 1:
                interval_df = base_df.copy()
            else:
                interval_df = make_interval(
                    base_df,
                    current_timestamp,
                    interval_number,
                )

            interval_start = time.monotonic()
            sent_this_interval = 0

            for row in interval_df.to_dict(orient="records"):
                if STOP_REQUESTED:
                    break

                elapsed = time.monotonic() - start

                if duration_limit is not None and elapsed >= duration_limit:
                    break

                if args.max_events is not None:
                    if total_sent >= args.max_events:
                        break

                producer.send(
                    args.topic,
                    key=str(row["transformer_id"]),
                    value=row,
                )

                total_sent += 1
                sent_this_interval += 1

                # Pace messages against the target rate.
                target_elapsed = sent_this_interval / args.rate
                actual_elapsed = time.monotonic() - interval_start
                if target_elapsed > actual_elapsed:
                    time.sleep(target_elapsed - actual_elapsed)

                if total_sent % 10_000 == 0:
                    elapsed = max(time.monotonic() - start, 0.001)
                    print(
                        f"Queued {total_sent:,} events | "
                        f"{total_sent / elapsed:,.1f} events/sec average"
                    )

            # Wait for outstanding sends and surface delivery errors.
            producer.flush()

            elapsed = max(time.monotonic() - start, 0.001)
            print(
                f"Interval {interval_number}: "
                f"{sent_this_interval:,} messages | "
                f"total {total_sent:,} | "
                f"average {total_sent / elapsed:,.1f} events/sec"
            )

            if STOP_REQUESTED:
                break

            if sent_this_interval < len(interval_df):
                break

            current_timestamp += pd.Timedelta(minutes=15)

        producer.flush()

    finally:
        producer.close()
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)

        elapsed = max(time.monotonic() - start, 0.001)

        print("\nPublisher stopped.")
        print(f"Messages queued: {total_sent:,}")
        print(f"Elapsed time: {elapsed:.1f} seconds")
        print(f"Average rate: {total_sent / elapsed:,.2f} events/sec")
        print(f"Topic: {args.topic}")
        print(
            "Verify broker and MinIO counts separately; queued messages "
            "are not proof of end-to-end ingestion."
        )


if __name__ == "__main__":
    main()

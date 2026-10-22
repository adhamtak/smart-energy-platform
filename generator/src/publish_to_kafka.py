
import argparse
import json
import time
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


def json_default(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def load_interval(data_dir, timestamp):
    """Load exactly one timestamp from every Parquet file."""
    readings_dir = data_dir / "readings"
    files = sorted(readings_dir.glob("part-*.parquet"))

    if not files:
        raise FileNotFoundError(f"No Parquet files found in {readings_dir}")

    target = pd.to_datetime(timestamp, utc=True)
    frames = []

    print(f"Scanning {len(files):,} Parquet files for {target}...")

    for index, path in enumerate(files, start=1):
        # Read only the columns needed for publishing.
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
        raise ValueError(
            f"No readings found for {target}. Check the timestamp."
        )

    df = pd.concat(frames, ignore_index=True)

    # Require exactly one reading per meter.
    duplicates = df["meter_id"].duplicated().sum()
    if duplicates:
        raise ValueError(
            f"Found {duplicates:,} duplicate meter readings at {target}."
        )

    df = df.sort_values(
        ["transformer_id", "meter_id"]
    ).reset_index(drop=True)

    df = df.astype(object).where(pd.notna(df), None)

    return df, len(files), target


def main():
    parser = argparse.ArgumentParser(
        description="Publish one smart-meter interval to Kafka."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing manifest.json and readings/.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Actually publish. Without this flag, preview only.",
    )
    parser.add_argument(
        "--topic",
        default="ami-readings-dev",
        help="Kafka topic to publish to.",
    )
    parser.add_argument(
        "--interval-timestamp",
        default="2026-07-01T00:00:00Z",
        help="UTC timestamp for the interval, e.g. 2026-07-01T00:15:00Z.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=60,
        help="Seconds over which to publish the interval (default: 60).",
    )

    args = parser.parse_args()

    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be greater than zero.")

    data_dir = args.input_dir.resolve()
    manifest_path = data_dir / "manifest.json"

    if not manifest_path.is_file():
        raise FileNotFoundError(f"manifest.json not found in {data_dir}")

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    if manifest.get("data_type") != "synthetic":
        raise ValueError("Expected a dataset marked as synthetic.")

    df, file_count, target = load_interval(
        data_dir, args.interval_timestamp
    )

    unique_meters = df["meter_id"].nunique()
    print(f"\nDataset: {data_dir}")
    print(f"Files scanned: {file_count:,}")
    print(f"Selected timestamp (UTC): {target}")
    print(f"Readings selected: {len(df):,}")
    print(f"Unique meters: {unique_meters:,}")
    print(f"Publish topic: {args.topic}")
    print(f"Target publishing duration: {args.interval_seconds:g} seconds")

    if len(df) != 100_000 or unique_meters != 100_000:
        raise ValueError(
            "Expected exactly 100,000 readings from 100,000 unique meters. "
            "Check the dataset before publishing."
        )

    if not args.publish:
        print("\nDRY RUN — nothing was sent to Kafka.")
        print("First 5 readings:")
        print(
            df[["reading_id", "event_timestamp", "transformer_id", "meter_id"]]
            .head(5)
            .to_string(index=False)
        )
        print("\nValidation passed. Add --publish to send these readings.")
        return

    from kafka import KafkaProducer

    bootstrap_servers = __import__("os").getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            default=json_default,
            allow_nan=False,
        ).encode("utf-8"),
    )

    interval = args.interval_seconds / len(df)
    start = time.monotonic()
    sent = 0

    try:
        for row in df.to_dict(orient="records"):
            producer.send(
                args.topic,
                key=str(row["transformer_id"]),
                value=row,
            )
            sent += 1

            if sent % 10_000 == 0:
                print(f"Queued {sent:,}/{len(df):,} readings")

            target_elapsed = sent * interval
            remaining = target_elapsed - (time.monotonic() - start)
            if remaining > 0:
                time.sleep(remaining)

        producer.flush()

        elapsed = time.monotonic() - start
        print(f"\nPublished {sent:,} readings in {elapsed:.2f} seconds.")
        print(f"Average rate: {sent / elapsed:.2f} messages/second")

    finally:
        producer.close()


if __name__ == "__main__":
    main()

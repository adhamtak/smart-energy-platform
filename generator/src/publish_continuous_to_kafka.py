
import argparse
import json
import os
import signal
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

from streaming_generator import StreamingSimulator


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "scale-test-100k-1day-v2"

STOP_REQUESTED = False


def request_stop(signum, frame):
    """Request a graceful shutdown."""
    global STOP_REQUESTED
    STOP_REQUESTED = True


def json_default(value):
    """Convert common NumPy and pandas values into JSON-compatible values."""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        return value.item()

    raise TypeError(f"Cannot serialize {type(value).__name__}")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate fresh synthetic AMI readings and publish them to Kafka."
        )
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Dataset directory containing dim_meter.parquet.",
    )

    parser.add_argument(
        "--topic",
        default="ami-readings-stream",
        help="Kafka topic to publish to.",
    )

    parser.add_argument(
        "--interval-timestamp",
        default="2026-07-01T00:00:00Z",
        help="Fallback starting event timestamp in UTC.",
    )

    parser.add_argument(
        "--start-time",
        default=None,
        help="Starting event timestamp in UTC; overrides --interval-timestamp.",
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
        help="Continue publishing until stopped or a configured limit is reached.",
    )

    parser.add_argument(
        "--duration-minutes",
        type=float,
        default=None,
        help="Optional wall-clock publishing duration in minutes.",
    )

    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Optional maximum number of messages to publish.",
    )

    parser.add_argument(
        "--publish",
        action="store_true",
        help="Actually publish messages to Kafka. Otherwise, perform a dry run.",
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

    return args


def validate_simulator(simulator):
    """Validate the expected number of meters."""
    if simulator.count != 100_000:
        raise ValueError(
            f"Expected 100,000 meters, found {simulator.count:,}."
        )


def show_dry_run(simulator, first_timestamp):
    """Generate and display one interval without publishing to Kafka."""
    sample = simulator.next_interval(first_timestamp, 1)

    if len(sample) != simulator.count:
        raise ValueError(
            f"Expected {simulator.count:,} readings, "
            f"generated {len(sample):,}."
        )

    if sample["meter_id"].nunique() != simulator.count:
        raise ValueError("Generated interval contains duplicate meter IDs.")

    print("\nDRY RUN: no messages will be sent.")
    print(f"Readings generated: {len(sample):,}")
    print("\nSample readings:")

    preview_columns = [
        "reading_id",
        "event_timestamp",
        "meter_id",
        "active_power_kw",
        "voltage_v",
    ]

    print(sample[preview_columns].head().to_string(index=False))
    print("\nValidation passed. Add --publish to send messages.")


def main():
    global STOP_REQUESTED

    args = parse_arguments()

    data_dir = args.input_dir.resolve()

    if not data_dir.is_dir():
        raise FileNotFoundError(
            f"Dataset directory does not exist: {data_dir}"
        )

    simulator = StreamingSimulator(data_dir)
    validate_simulator(simulator)

    first_timestamp = pd.to_datetime(
        args.start_time or args.interval_timestamp,
        utc=True,
    )

    print("\nSmart Energy Intelligence Platform")
    print("-" * 42)
    print(f"Grid dimensions: {data_dir}")
    print(f"Start timestamp: {first_timestamp}")
    print(f"Meters: {simulator.count:,}")
    print(f"Kafka topic: {args.topic}")
    print(f"Target rate: {args.rate:,.0f} events/second")

    if not args.publish:
        show_dry_run(simulator, first_timestamp)
        return

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            default=json_default,
            allow_nan=False,
        ).encode("utf-8"),
    )

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

            # Generate fresh readings for this simulated 15-minute interval.
            interval_df = simulator.next_interval(
                current_timestamp,
                interval_number,
            )

            if len(interval_df) != simulator.count:
                raise ValueError(
                    f"Interval {interval_number} generated "
                    f"{len(interval_df):,} readings; "
                    f"expected {simulator.count:,}."
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

                # Pace messages against the configured target rate.
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

            # Do not start another interval if the current one was partial.
            if sent_this_interval < len(interval_df):
                break

            # Advance simulated event time by 15 minutes.
            current_timestamp += pd.Timedelta(minutes=15)

    finally:
        try:
            producer.flush()
        finally:
            producer.close()

            signal.signal(signal.SIGINT, previous_sigint)
            signal.signal(signal.SIGTERM, previous_sigterm)

        elapsed = max(time.monotonic() - start, 0.001)

        print("\nPublisher stopped.")
        print(f"Messages queued: {total_sent:,}")
        print(f"Elapsed wall-clock time: {elapsed:.1f} seconds")
        print(f"Average publishing rate: {total_sent / elapsed:,.2f} events/sec")
        print(f"Kafka topic: {args.topic}")
        print(
            "Verify Kafka consumption and MinIO ingestion separately; "
            "queued messages do not prove end-to-end ingestion."
        )


if __name__ == "__main__":
    main()

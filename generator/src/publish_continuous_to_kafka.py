"""Publish synthetic AMI readings to Kafka, continuing the historical dataset.

* The stream starts on the day after the last historical day (read from the
  dataset's manifest.json) unless --start-time is given.
* Every reading is a pure function of (seed, timestamp), so a restart that
  replays an interval produces identical rows (the consumer/Silver dedupe on
  reading_id).
* After the 23:45 interval of each day the producer sends one DAY_CLOSED
  control message to EVERY partition of the topic. Kafka preserves order per
  partition, so when the consumer has seen the control message on all
  partitions, all data for that day has been consumed.
* Progress is checkpointed after each complete interval so a restart resumes
  at the next interval.
"""

import argparse
import json
import os
import signal
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

from streaming_generator import (
    INTERVALS_PER_DAY,
    StreamingSimulator,
    is_last_interval_of_day,
    stream_start_from_manifest,
)


def find_project_root(start: Path) -> Path:
    start = Path(start).resolve()
    for candidate in (start, *start.parents):
        if any((candidate / marker).exists()
               for marker in (".env", "docker-compose.yml", ".git")):
            return candidate
    return start


PROJECT_ROOT = find_project_root(Path(__file__).parent)
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "scale-test-100k-1day-v2"
DEFAULT_TOPIC = os.getenv("KAFKA_INGEST_TOPIC", "ami-readings-stream")

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


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Generate fresh synthetic AMI readings and publish them to Kafka."
        )
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Historical dataset directory (dims + manifest.json).",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic (default: $KAFKA_INGEST_TOPIC or ami-readings-stream).",
    )
    parser.add_argument(
        "--start-time",
        default=None,
        help=(
            "First interval to publish (UTC, 15-minute aligned). Default: "
            "resume from the checkpoint, else the day after the historical data."
        ),
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
        help="Keep publishing interval after interval. Without it, only one "
             "interval is published.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Stop after this many complete simulated days (implies --continuous).",
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
        "--checkpoint",
        type=Path,
        default=None,
        help="Checkpoint file (default: <input-dir>/.stream_checkpoint.json).",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Ignore an existing checkpoint and start from the default start.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Actually publish messages to Kafka. Otherwise, perform a dry run.",
    )

    args = parser.parse_args(argv)

    if args.days is not None:
        if args.days <= 0:
            parser.error("--days must be positive.")
        args.continuous = True

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


def to_utc(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def resolve_start(args, data_dir: Path, checkpoint_path: Path) -> pd.Timestamp:
    if args.start_time:
        return to_utc(args.start_time)

    if checkpoint_path.is_file() and not args.reset_checkpoint:
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        return to_utc(checkpoint["next_timestamp"])

    return to_utc(stream_start_from_manifest(data_dir))


def write_checkpoint(path: Path, next_timestamp: pd.Timestamp) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps({"next_timestamp": next_timestamp.isoformat()}),
        encoding="utf-8",
    )
    tmp.replace(path)


def topic_partitions(producer, topic: str) -> list:
    """Partition numbers of the topic (waits briefly for metadata)."""
    for _ in range(20):
        partitions = producer.partitions_for(topic)
        if partitions:
            return sorted(partitions)
        time.sleep(0.5)
    raise RuntimeError(f"Could not read partitions for topic {topic!r}.")


def send_day_closed(producer, topic, partitions, day: str, expected_rows: int):
    """Tell the consumer that every reading for `day` has been published."""
    futures = []
    for partition in partitions:
        futures.append(
            producer.send(
                topic,
                key=f"day-closed-{day}-{partition}",
                value={
                    "type": "DAY_CLOSED",
                    "dt": day,
                    "expected_rows": expected_rows,
                    "partitions": len(partitions),
                },
                partition=partition,
            )
        )
    producer.flush()
    for future in futures:
        future.get(timeout=30)


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


def main(argv=None):
    global STOP_REQUESTED

    args = parse_arguments(argv)

    data_dir = args.input_dir.resolve()
    if not data_dir.is_dir():
        raise FileNotFoundError(f"Dataset directory does not exist: {data_dir}")

    checkpoint_path = (
        args.checkpoint or data_dir / ".stream_checkpoint.json"
    )

    simulator = StreamingSimulator(data_dir)
    if simulator.count <= 0:
        raise ValueError("Dataset contains no meters.")

    resumed = (
        not args.start_time
        and checkpoint_path.is_file()
        and not args.reset_checkpoint
    )
    first_timestamp = resolve_start(args, data_dir, checkpoint_path)

    if first_timestamp < simulator.stream_start:
        raise ValueError(
            f"Start time {first_timestamp.isoformat()} is before the stream "
            f"start {simulator.stream_start.isoformat()} (the day after the "
            "historical data); it would overlap history."
        )
    if first_timestamp != first_timestamp.floor("15min"):
        raise ValueError("Start time must be aligned to a 15-minute interval.")

    expected_rows_per_day = simulator.count * INTERVALS_PER_DAY

    print("\nSmart Energy Intelligence Platform")
    print("-" * 42)
    print(f"Grid dimensions: {data_dir}")
    print(f"Start timestamp: {first_timestamp.isoformat()}")
    print(f"Meters: {simulator.count:,}")
    print(f"Kafka topic: {args.topic}")
    print(f"Target rate: {args.rate:,.0f} events/second")
    if resumed:
        print(f"Resuming from checkpoint: {checkpoint_path}")
    elif first_timestamp.floor("D") != first_timestamp:
        print("WARNING: starting mid-day; the first day will be incomplete "
              "unless earlier intervals were published before.")

    if not args.publish:
        show_dry_run(simulator, first_timestamp)
        return

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        linger_ms=20,
        batch_size=128 * 1024,
        compression_type="gzip",
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            default=json_default,
            allow_nan=False,
        ).encode("utf-8"),
    )

    partitions = topic_partitions(producer, args.topic)
    print(f"Topic partitions: {len(partitions)}")

    # Delivery failures surface through these callbacks (flush() alone does
    # not raise them).
    errors = []

    def on_error(exc):
        errors.append(exc)

    def raise_if_errors():
        if errors:
            raise RuntimeError(
                f"{len(errors)} messages failed to deliver; first error: "
                f"{errors[0]!r}"
            )

    STOP_REQUESTED = False
    previous_sigint = signal.signal(signal.SIGINT, request_stop)
    previous_sigterm = signal.signal(signal.SIGTERM, request_stop)

    start = time.monotonic()
    total_sent = 0
    interval_number = 0
    days_closed = 0
    current_timestamp = first_timestamp
    pace_every = max(1, int(args.rate // 50))

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

            if args.max_events is not None and total_sent >= args.max_events:
                break

            interval_number += 1

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

                if duration_limit is not None:
                    if time.monotonic() - start >= duration_limit:
                        break

                if args.max_events is not None and total_sent >= args.max_events:
                    break

                producer.send(
                    args.topic,
                    key=row["transformer_id"],
                    value=row,
                ).add_errback(on_error)

                total_sent += 1
                sent_this_interval += 1

                # Pace against the target rate, checking every few messages.
                if sent_this_interval % pace_every == 0:
                    target_elapsed = sent_this_interval / args.rate
                    actual_elapsed = time.monotonic() - interval_start
                    if target_elapsed > actual_elapsed:
                        time.sleep(target_elapsed - actual_elapsed)

            # Wait for outstanding sends, then surface any delivery error.
            producer.flush()
            raise_if_errors()

            elapsed = max(time.monotonic() - start, 0.001)
            print(
                f"Interval {interval_number} "
                f"({current_timestamp.isoformat()}): "
                f"{sent_this_interval:,} messages | "
                f"total {total_sent:,} | "
                f"average {total_sent / elapsed:,.1f} events/sec"
            )

            # A partial interval is never checkpointed: a restart replays it
            # in full (identical rows, deduplicated downstream).
            if sent_this_interval < len(interval_df):
                break

            if is_last_interval_of_day(current_timestamp):
                day = current_timestamp.strftime("%Y-%m-%d")
                send_day_closed(
                    producer, args.topic, partitions, day,
                    expected_rows_per_day,
                )
                days_closed += 1
                print(f"Day closed: {day} ({expected_rows_per_day:,} rows)")

            current_timestamp += pd.Timedelta(minutes=15)
            write_checkpoint(checkpoint_path, current_timestamp)

            if not args.continuous:
                break

            if args.days is not None and days_closed >= args.days:
                break

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
        print(f"Days closed: {days_closed}")
        print(f"Elapsed wall-clock time: {elapsed:.1f} seconds")
        print(f"Average publishing rate: {total_sent / elapsed:,.2f} events/sec")
        print(f"Next interval to publish: {current_timestamp.isoformat()}")
        print(f"Kafka topic: {args.topic}")
        print(
            "Verify Kafka consumption and MinIO ingestion separately; "
            "queued messages do not prove end-to-end ingestion."
        )


if __name__ == "__main__":
    main()
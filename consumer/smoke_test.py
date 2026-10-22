
import argparse
import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

REQUIRED_FIELDS = {
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


def main():
    parser = argparse.ArgumentParser(
        description="Validate a small sample of smart-meter Kafka messages."
    )
    parser.add_argument("--topic", default="ami-readings-dev")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    if args.limit < 1:
        parser.error("--limit must be at least 1")

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    valid = 0
    invalid = 0
    seen_meters = set()

    print(f"Topic: {args.topic}")
    print(f"Reading up to {args.limit} messages...")
    print("Starting from the earliest available offset.")

    try:
        for message in consumer:
            try:
                record = message.value

                if not isinstance(record, dict):
                    raise ValueError("Message is not a JSON object")

                missing = REQUIRED_FIELDS - set(record.keys())
                if missing:
                    raise ValueError(
                        f"Missing fields: {sorted(missing)}"
                    )

                valid += 1
                seen_meters.add(record["meter_id"])

            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                invalid += 1
                print(f"Invalid message: {exc}")

            if valid + invalid >= args.limit:
                break

        print("\nConsumer smoke-test results")
        print(f"Messages checked: {valid + invalid}")
        print(f"Valid messages: {valid}")
        print(f"Invalid messages: {invalid}")
        print(f"Distinct meters in sample: {len(seen_meters)}")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()

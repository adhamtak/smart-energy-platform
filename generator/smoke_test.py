
import io
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from datetime import date

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaProducer


# Load .env without requiring python-dotenv.
ROOT = Path(__file__).resolve().parents[1]
env_file = ROOT / ".env"

if not env_file.exists():
    raise FileNotFoundError(
        f"Missing {env_file}. Copy .env.example to .env first."
    )

for line in env_file.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

BUCKET = os.getenv("S3_BUCKET", "lakehouse")
S3_ENDPOINT = "http://localhost:9000"
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = os.getenv("TOPIC_READINGS", "ami-readings")

# Connect to MinIO using credentials from .env.
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.environ["S3_ROOT_USER"],
    aws_secret_access_key=os.environ["S3_ROOT_PASSWORD"],
    region_name="us-east-1",
)

# Check that the expected bucket exists; do not silently create a
# differently named bucket if the shared environment is misconfigured.
existing_buckets = {
    item["Name"] for item in s3.list_buckets()["Buckets"]
}
if BUCKET not in existing_buckets:
    raise RuntimeError(
        f"Bucket '{BUCKET}' was not found. Check MinIO and .env."
    )

# Tiny, explicitly synthetic reference data.
region = [{"region_id": "NCAI", "region_name": "North Cairo"}]

substation = [{
    "substation_id": "SUB_NCAI_01",
    "name": "Synthetic Test Substation",
    "region_id": "NCAI",
    "city": "Cairo",
    "primary_kv": 66.0,
    "secondary_kv": 11.0,
    "capacity_mva": 5.0,
    "status": "ACTIVE",
}]

transformer = [{
    "transformer_id": "TX_NCAI_0001",
    "substation_id": "SUB_NCAI_01",
    "region_id": "NCAI",
    "rated_capacity_kva": 500.0,
    "nominal_voltage_v": 380.0,
    "install_date": "2020-01-01",
    "status": "ACTIVE",
    "install_date": date(2020, 1, 1),

}]

meters = [
    {
        "meter_id": f"MTR{i:06d}",
        "transformer_id": "TX_NCAI_0001",
        "customer_type": "RESIDENTIAL",
        "tariff_category": None,
        "phase_type": "SINGLE_PHASE",
        "install_date": "2022-01-01",
        "status": "ACTIVE",
        "install_date": date(2022, 1, 1),
    }
    for i in (1, 2)
]

# Create 12 quarter-hour readings for each meter: 24 total.
start = datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc)
readings = []

for meter_index, meter in enumerate(meters):
    for interval in range(12):
        timestamp = start + timedelta(minutes=15 * interval)
        power_kw = round(0.5 + meter_index * 0.2 + interval * 0.03, 3)
        reactive_kvar = round(power_kw * 0.25, 3)
        apparent_kva = (power_kw**2 + reactive_kvar**2) ** 0.5

        readings.append({
            "reading_id": (
                f"READ_{meter['meter_id']}_"
                f"{timestamp.strftime('%Y%m%d%H%M')}"
            ),
            "event_timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "meter_id": meter["meter_id"],
            "transformer_id": meter["transformer_id"],
            "voltage_v": round(220.0 + (interval % 5) * 0.4, 2),
            "active_power_kw": power_kw,
            "reactive_power_kvar": reactive_kvar,
            "energy_kwh": round(power_kw * 0.25, 5),
            "power_factor": round(power_kw / apparent_kva, 4),
            "schema_version": 1,
        })

def parquet_bytes(records):
    """Serialize records as Parquet in memory."""
    table = pa.Table.from_pylist(records)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer.getvalue()

def upload_parquet(prefix, records):
    key = f"{prefix}.parquet"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=parquet_bytes(records),
        ContentType="application/octet-stream",
    )
    print(f"Uploaded s3://{BUCKET}/{key} ({len(records)} rows)")

# Upload the reference dimensions.
upload_parquet("reference/dim_region/part-00000", region)
upload_parquet("reference/dim_substation/part-00000", substation)
upload_parquet("reference/dim_transformer/part-00000", transformer)
upload_parquet("reference/dim_meter/part-00000", meters)

# Upload historical readings into the Bronze date partition.
upload_parquet(
    "bronze/smart_meter_readings/year=2026/month=07/day=01/part-00000",
    readings,
)

# Publish the same readings to Kafka, keyed by transformer_id.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
)

try:
    for reading in readings:
        producer.send(
            TOPIC,
            key=reading["transformer_id"],
            value=reading,
        ).get(timeout=15)

    producer.flush(timeout=15)
finally:
    producer.close()

print(f"Published {len(readings)} readings to Kafka topic '{TOPIC}'.")
print("Smoke test completed. All readings are synthetic test data.")

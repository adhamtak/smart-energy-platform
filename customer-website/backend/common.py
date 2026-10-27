"""Shared helpers for the customer portal (settings, MinIO access, passwords, data prep)."""
import hashlib
import hmac
import json
import os
import re
import secrets
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PORTAL_DIR = Path(__file__).resolve().parent
USERS_FILE = PORTAL_DIR / "users.json"

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

BUCKET = os.getenv("S3_BUCKET", "lakehouse")
RUNS_BASE = os.getenv("GEN_S3_PREFIX", "dev/historical-generator").strip("/") + "/runs/"
INCIDENTS_PREFIX = os.getenv("INCIDENTS_PREFIX", "gold/fact_incidents").strip("/")
VOLTAGE_LOW_V = float(os.getenv("VOLTAGE_LOW_V", "210"))
METER_RE = re.compile(r"^MTR\d{6}$")


# ---------------------------------------------------------------- MinIO / DuckDB
def s3_endpoint_url():
    return os.getenv("S3_ENDPOINT_URL") or f"http://localhost:{os.getenv('S3_API_PORT', '9000')}"


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint_url(),
        aws_access_key_id=os.environ["S3_ROOT_USER"],
        aws_secret_access_key=os.environ["S3_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def lake_path(prefix, rel):
    """s3://bucket/<prefix>/<rel>, also correct when prefix is empty (data at bucket root)."""
    return f"s3://{BUCKET}/" + (f"{prefix}/" if prefix else "") + rel


# Folders the data can live in (matched anywhere in the bucket, so a run prefix is fine too):
#   bronze/smart_meter_readings/...                  streaming + shared bronze
#   bronze/historical/run_id=<id>/readings/...       historical upload
#   reference/dim_meter/...                          meter dimension
_RE_BRONZE = re.compile(r"^(?:.*/)?bronze/smart_meter_readings/")
_RE_HISTORY = re.compile(r"^(?:.*/)?bronze/historical/run_id=[^/]+/readings/")
_RE_DIM = re.compile(r"^(?:.*/)?reference/dim_meter/")


def _parquet_keys(limit=300_000):
    seen = 0
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            seen += 1
            if obj["Key"].endswith(".parquet"):
                yield obj["Key"]
            if seen >= limit:
                return


def discover_layout():
    """Scan the bucket once and return where the readings and dim_meter really are:
    {"readings": [s3 globs], "dim_meter": s3 glob or None}.
    Overrides (.env): PORTAL_READINGS_GLOBS (comma separated), PORTAL_DIM_METER_GLOB."""
    forced_readings = os.getenv("PORTAL_READINGS_GLOBS", "").strip()
    forced_dim = os.getenv("PORTAL_DIM_METER_GLOB", "").strip()

    bronze, history, dims = set(), set(), set()
    if not (forced_readings and forced_dim):
        for key in _parquet_keys():
            for pattern, found in ((_RE_BRONZE, bronze), (_RE_HISTORY, history), (_RE_DIM, dims)):
                match = pattern.match(key)
                if match:
                    found.add(match.group(0))

    if forced_readings:
        readings = [g.strip() for g in forced_readings.split(",") if g.strip()]
    else:
        roots = sorted(bronze, reverse=True)[:3] + sorted(history, reverse=True)[:1]
        readings = [f"s3://{BUCKET}/{root}**/*.parquet" for root in roots]

    if forced_dim:
        dim = forced_dim
    elif dims:
        # prefer the shared top-level reference/ folder, else the newest run's copy
        root = min(sorted(dims, reverse=True), key=lambda r: r.count("/"))
        dim = f"s3://{BUCKET}/{root}*.parquet"
    else:
        dim = None

    if not readings:
        raise RuntimeError(
            f"No readings parquet found in bucket '{BUCKET}' (looked for bronze/smart_meter_readings/ "
            "and bronze/historical/run_id=*/readings/). Run: python backend/where_is_my_data.py"
        )
    return {"readings": readings, "dim_meter": dim}


def readings_source(globs):
    """SQL fragment that reads all the readings globs as one table."""
    quoted = ",".join("'" + g.replace("'", "''") + "'" for g in globs)
    return f"read_parquet([{quoted}], hive_partitioning=false, union_by_name=true)"


def duckdb_connection():
    import duckdb

    endpoint = urlparse(s3_endpoint_url())
    q = lambda value: str(value).replace("'", "''")
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("INSTALL httpfs; LOAD httpfs;")  # first run downloads the extension (needs internet)
    con.execute(f"SET s3_endpoint='{q(endpoint.netloc)}'")
    con.execute(f"SET s3_access_key_id='{q(os.environ['S3_ROOT_USER'])}'")
    con.execute(f"SET s3_secret_access_key='{q(os.environ['S3_ROOT_PASSWORD'])}'")
    con.execute(f"SET s3_use_ssl={'true' if endpoint.scheme == 'https' else 'false'}")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_region='us-east-1'")
    return con


# ---------------------------------------------------------------- users / passwords
def hash_password(password):
    salt = secrets.token_hex(8)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000).hex()
    return f"{salt}${digest}"


def verify_password(password, stored):
    try:
        salt, digest = stored.split("$")
    except ValueError:
        return False
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000).hex()
    return hmac.compare_digest(candidate, digest)


def load_users():
    if not USERS_FILE.is_file():
        return {}
    return json.loads(USERS_FILE.read_text(encoding="utf-8"))


def can_access(user_record, meter_id):
    meters = user_record.get("meters", [])
    return "*" in meters or meter_id in meters


# ---------------------------------------------------------------- data prep
def prepare(df):
    """Add Cairo local time, date and a computed power factor."""
    import numpy as np

    df = df.copy()
    df["ts_local"] = (
        df["ts"].dt.tz_localize("UTC").dt.tz_convert("Africa/Cairo").dt.tz_localize(None)
    )
    df["date"] = df["ts_local"].dt.date
    apparent = np.sqrt(df["active_power_kw"] ** 2 + df["reactive_power_kvar"] ** 2)
    df["power_factor"] = (df["active_power_kw"] / apparent).where(apparent > 0)
    return df


def summarize(df):
    import pandas as pd

    peak = df.loc[df["active_power_kw"].idxmax()]
    # Days of data = readings / 96 (15-minute readings). Using first-to-last timestamp would count
    # the empty months between two separate loads (e.g. a July history + a December stream).
    n_days = max(len(df) / 96, 1 / 96)
    n_utc_days = int(df["ts"].dt.date.nunique())
    total = float(df["energy_kwh"].sum())
    return {
        "n_readings": int(len(df)),
        "n_days": float(n_days),
        "multi_day": n_utc_days > 1,
        "total_kwh": total,
        "avg_daily_kwh": total / n_days,
        "peak_kw": float(peak["active_power_kw"]),
        "peak_time": peak["ts_local"],
        "min_voltage": float(df["voltage_v"].min()),
        "avg_voltage": float(df["voltage_v"].mean()),
        "low_voltage_intervals": int((df["voltage_v"] < VOLTAGE_LOW_V).sum()),
        "avg_power_factor": float(df["power_factor"].mean()),
    }

"""
Live incidents dashboard (Streamlit) for the Smart Energy platform.

Reads the fact_incidents Parquet that consumer/alerts_to_incidents.py writes to MinIO
and shows counts, charts and the latest incidents. Run from the project root:

    pip install streamlit boto3 pandas pyarrow python-dotenv
    streamlit run dashboard/app.py

Settings (optional, from .env): S3_BUCKET, S3_ENDPOINT_URL, S3_API_PORT,
S3_ROOT_USER, S3_ROOT_PASSWORD, INCIDENTS_PREFIX (default gold/fact_incidents).
"""
import io
import os
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SEVERITIES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
SEVERITY_ICONS = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}


def settings():
    prefix = os.getenv("INCIDENTS_PREFIX", "gold/fact_incidents").strip("/")
    return {
        "bucket": os.getenv("S3_BUCKET", "lakehouse"),
        "key": f"{prefix}/part-00000.parquet",
        "endpoint": os.getenv(
            "S3_ENDPOINT_URL",
            f"http://localhost:{os.getenv('S3_API_PORT', '9000')}",
        ),
    }


def read_incidents(s3, bucket, key):
    """Download the Parquet file and return it as a DataFrame."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def apply_filters(frame, types, severities, regions):
    """Keep only rows matching the chosen values (an empty choice keeps all)."""
    if types:
        frame = frame[frame["anomaly_type"].isin(types)]
    if severities:
        frame = frame[frame["severity"].isin(severities)]
    if regions:
        frame = frame[frame["region_id"].isin(regions)]
    return frame


def severity_counts(frame):
    counts = frame["severity"].value_counts()
    return {name: int(counts.get(name, 0)) for name in SEVERITIES}


def latest(frame, n=200):
    return frame.sort_values("detected_at", ascending=False).head(n)


def main():
    import boto3
    import streamlit as st
    from botocore.config import Config

    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass

    cfg = settings()
    st.set_page_config(page_title="Smart Energy: Incidents", layout="wide")
    st.title("⚡ Smart Energy: Incidents")
    st.caption("Synthetic data. Source: " f"s3://{cfg['bucket']}/{cfg['key']}")

    s3 = boto3.client(
        "s3",
        endpoint_url=cfg["endpoint"],
        aws_access_key_id=os.getenv("S3_ROOT_USER"),
        aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )

    auto = st.sidebar.toggle("Auto refresh", value=True)
    seconds = st.sidebar.slider("Every (seconds)", 5, 60, 10)

    try:
        data = read_incidents(s3, cfg["bucket"], cfg["key"])
    except Exception as error:  # MinIO down, file not written yet, wrong credentials
        st.info(
            "No incidents file yet. Start the processor, the publisher and "
            "consumer/alerts_to_incidents.py, then wait a few seconds."
        )
        st.caption(f"Details: {error}")
        data = None

    if data is not None and not data.empty:
        types = st.sidebar.multiselect("Type", sorted(data["anomaly_type"].dropna().unique()))
        severities = st.sidebar.multiselect("Severity", [s for s in SEVERITIES if s in set(data["severity"])])
        regions = st.sidebar.multiselect("Region", sorted(data["region_id"].dropna().unique()))
        view = apply_filters(data, types, severities, regions)

        counts = severity_counts(view)
        columns = st.columns(len(SEVERITIES) + 1)
        columns[0].metric("Incidents", f"{len(view):,}")
        for column, name in zip(columns[1:], SEVERITIES):
            column.metric(f"{SEVERITY_ICONS[name]} {name}", f"{counts[name]:,}")

        left, middle, right = st.columns(3)
        left.subheader("By type")
        left.bar_chart(view["anomaly_type"].value_counts())
        middle.subheader("By region")
        middle.bar_chart(view["region_id"].value_counts())
        right.subheader("By start time")
        right.bar_chart(view.groupby("start_time").size())

        st.subheader("Latest incidents")
        st.dataframe(
            latest(view)[[
                "detected_at", "severity", "anomaly_type", "asset_id",
                "region_id", "observed_value", "threshold", "status", "incident_id",
            ]],
            use_container_width=True,
            hide_index=True,
        )
    elif data is not None:
        st.info("The incidents file exists but has no rows yet.")

    if auto:
        time.sleep(seconds)
        st.rerun()


if __name__ == "__main__":
    main()

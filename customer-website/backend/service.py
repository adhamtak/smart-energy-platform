"""Data logic for the portal API (no web framework in here, so it is easy to test)."""
import json
import threading
import time

import pandas as pd

from common import (
    BUCKET, INCIDENTS_PREFIX, VOLTAGE_LOW_V, discover_layout, duckdb_connection, prepare,
    readings_source, summarize,
)

# ---------------------------------------------------------------- tiny TTL cache
_cache, _lock = {}, threading.Lock()


def cached(key, ttl_seconds, compute):
    now = time.monotonic()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl_seconds:
            return hit[1]
    value = compute()
    with _lock:
        _cache[key] = (now, value)
    return value


def clear_cache():
    with _lock:
        _cache.clear()


def records(frame):
    """DataFrame -> list of JSON-safe dicts (NaN becomes null, timestamps become ISO strings)."""
    return json.loads(frame.to_json(orient="records", date_format="iso"))


# ---------------------------------------------------------------- MinIO reads (DuckDB)
def _query(sql, params=()):
    con = duckdb_connection()  # one connection per call: DuckDB connections are not thread-safe
    try:
        return con.execute(sql, list(params)).df()
    finally:
        con.close()


def fetch_readings(layout, meter_id):
    # Impossible values are dropped and duplicate reading_ids collapsed, as the data contract says.
    return _query(
        f"""
        SELECT reading_id,
               CAST(CAST(event_timestamp AS TIMESTAMPTZ) AS TIMESTAMP) AS ts,
               voltage_v, active_power_kw, reactive_power_kvar, energy_kwh
        FROM {readings_source(layout["readings"])}
        WHERE meter_id = ? AND voltage_v > 0 AND active_power_kw >= 0 AND energy_kwh >= 0
        QUALIFY row_number() OVER (PARTITION BY reading_id ORDER BY reading_id) = 1
        ORDER BY ts
        """,
        [meter_id],
    )


def fetch_meter_info(layout, meter_id):
    if not layout["dim_meter"]:
        return {}
    try:
        frame = _query(
            f"SELECT * FROM read_parquet('{layout['dim_meter']}') WHERE meter_id = ?", [meter_id]
        )
    except Exception:  # dim_meter unreadable: everything else still works
        return {}
    return records(frame.head(1))[0] if not frame.empty else {}


def fetch_incidents(asset_ids):
    path = f"s3://{BUCKET}/{INCIDENTS_PREFIX}/*.parquet"
    marks = ",".join("?" * len(asset_ids))
    try:
        frame = _query(
            f"SELECT * FROM read_parquet('{path}') WHERE asset_id IN ({marks}) "
            "ORDER BY detected_at DESC LIMIT 100",
            list(asset_ids),
        )
    except Exception:  # no incidents written yet
        return []
    return records(frame)


# ---------------------------------------------------------------- what the API returns
class NoData(Exception):
    pass


def get_layout():
    return cached("layout", 600, discover_layout)


def get_frame(meter_id):
    layout = get_layout()
    raw = cached(("readings", tuple(layout["readings"]), meter_id), 300, lambda: fetch_readings(layout, meter_id))
    if raw.empty:
        raise NoData(f"No readings found for {meter_id}.")
    return prepare(raw)


def build_payload(meter_id):
    layout = get_layout()
    df = get_frame(meter_id)
    info = cached(("info", layout["dim_meter"], meter_id), 600, lambda: fetch_meter_info(layout, meter_id))
    s = summarize(df)

    assets = [meter_id] + ([str(info["transformer_id"])] if info.get("transformer_id") else [])
    incidents = cached(("incidents", tuple(assets)), 60, lambda: fetch_incidents(assets))

    readings = pd.DataFrame(
        {
            "t": df["ts_local"].dt.strftime("%Y-%m-%dT%H:%M"),
            "voltage": df["voltage_v"].round(1),
            "kw": df["active_power_kw"].round(3),
            "kwh": df["energy_kwh"].round(4),
            "pf": df["power_factor"].round(3),
        }
    )
    # only hours that have data, so two separate periods don't produce months of zero hours
    hourly = df.groupby(df["ts_local"].dt.floor("h"))["energy_kwh"].sum().round(3)
    hourly = pd.DataFrame({"t": hourly.index.strftime("%Y-%m-%dT%H:%M"), "kwh": hourly.values})

    daily = []
    if s["multi_day"]:
        by_day = df.groupby("date")["energy_kwh"].sum().round(2)
        daily = [{"date": str(d), "kwh": float(v)} for d, v in by_day.items()]

    return {
        "meter_id": meter_id,
        "info": info,
        "summary": {
            "total_kwh": round(s["total_kwh"], 2),
            "avg_daily_kwh": round(s["avg_daily_kwh"], 2),
            "days": round(s["n_days"], 2),
            "multi_day": s["multi_day"],
            "peak_kw": round(s["peak_kw"], 3),
            "peak_time": s["peak_time"].strftime("%Y-%m-%d %H:%M"),
            "min_voltage": round(s["min_voltage"], 1),
            "avg_voltage": round(s["avg_voltage"], 1),
            "avg_power_factor": round(s["avg_power_factor"], 3),
            "low_voltage_intervals": s["low_voltage_intervals"],
            "n_readings": s["n_readings"],
        },
        "low_voltage_threshold": VOLTAGE_LOW_V,
        "readings": records(readings),
        "hourly": records(hourly),
        "daily": daily,
        "incidents": incidents,
        "timezone": "Africa/Cairo",
    }


def readings_csv(meter_id):
    df = get_frame(meter_id)
    cols = ["ts_local", "voltage_v", "active_power_kw", "reactive_power_kvar", "energy_kwh", "power_factor"]
    return df[cols].rename(columns={"ts_local": "time_cairo"}).to_csv(index=False)

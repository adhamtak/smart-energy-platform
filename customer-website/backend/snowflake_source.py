"""Reads the portal's data straight from Snowflake (read-only use).

Switch it on in backend/.env:   DATA_BACKEND=snowflake   (default is the MinIO lakehouse)

Which tables/views and columns are used is described in warehouse_mapping.json (copy it from
warehouse_mapping.example.json). A source can be a TABLE or a VIEW, and every logical column can be a
column name or a SQL expression, so you only edit the JSON when the batch team's names differ.
Credentials come from environment variables only; nothing secret is stored in the code.
"""
import json
import os
import re
import threading
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parent

# What the portal needs. Left side = logical name used by service.py; right side = default Snowflake
# column (follows the data contract). Change them in warehouse_mapping.json, not here.
DEFAULT_MAPPING = {
    "readings": {
        "table": "SILVER_SMART_METER_READINGS",
        "columns": {
            "ts": "EVENT_TIMESTAMP",
            "meter_id": "METER_ID",
            "voltage_v": "VOLTAGE_V",
            "active_power_kw": "ACTIVE_POWER_KW",
            "reactive_power_kvar": "REACTIVE_POWER_KVAR",
            "energy_kwh": "ENERGY_KWH",
        },
        # Impossible values are dropped (data contract). Remove it if the warehouse already cleaned them.
        "where": "VOLTAGE_V > 0 AND ACTIVE_POWER_KW >= 0 AND ENERGY_KWH >= 0",
        # Collapses duplicate readings (same READING_ID). Set to None if the table is already clean.
        "dedupe_by": "READING_ID",
    },
    "dim_meter": {
        "table": "SILVER_DIM_METER",
        "columns": {
            "meter_id": "METER_ID",
            "transformer_id": "TRANSFORMER_ID",
            "customer_type": "CUSTOMER_TYPE",
            "phase_type": "PHASE_TYPE",
            "status": "STATUS",
        },
    },
    "incidents": {
        # None = there is no incidents table in the warehouse yet, so the portal shows no incidents and
        # runs no query for them. When the batch team creates one, put its name here (or in
        # warehouse_mapping.json), for example "MARTS.FACT_INCIDENTS".
        "table": None,
        "columns": {
            "incident_id": "INCIDENT_ID",
            "detected_at": "DETECTED_AT",
            "severity": "SEVERITY",
            "anomaly_type": "ANOMALY_TYPE",
            "asset_id": "ASSET_ID",
            "status": "STATUS",
            "observed_value": "OBSERVED_VALUE",
            "threshold": "THRESHOLD",
        },
    },
}
NUMERIC = ("voltage_v", "active_power_kw", "reactive_power_kvar", "energy_kwh", "observed_value", "threshold")
_NAME_RE = re.compile(r'^[A-Za-z0-9_$."]+$')
_local = threading.local()  # one connection per worker thread


def enabled() -> bool:
    return os.getenv("DATA_BACKEND", "lakehouse").strip().lower() == "snowflake"


def load_mapping() -> dict:
    mapping = json.loads(json.dumps(DEFAULT_MAPPING))
    path = Path(os.getenv("SNOWFLAKE_MAPPING_FILE", BACKEND_DIR / "warehouse_mapping.json"))
    if path.is_file():
        for name, spec in json.loads(path.read_text(encoding="utf-8")).items():
            if name.startswith("_") or name not in mapping:
                continue
            mapping[name].update({k: v for k, v in spec.items() if k != "columns"})
            mapping[name]["columns"].update(spec.get("columns", {}))
    for name, spec in mapping.items():
        if spec["table"] is None and name == "incidents":
            continue  # optional source: not configured
        if not spec["table"] or not _NAME_RE.match(spec["table"]):
            raise ValueError(f"Invalid table/view name for '{name}': {spec['table']!r}")
    return mapping


def _select(spec, logical_columns):
    return ", ".join(f"{spec['columns'][c]} AS {c}" for c in logical_columns)


# ------------------------------------------------------------------ connection
def _connect():
    import snowflake.connector as sf

    sf.paramstyle = "qmark"  # use ? placeholders
    env = os.environ.get
    missing = [n for n in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA")
               if not env(n)]
    if not (env("SNOWFLAKE_PASSWORD") or env("SNOWFLAKE_PRIVATE_KEY_PATH")):
        missing.append("SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
    if missing:
        raise RuntimeError("DATA_BACKEND=snowflake but these settings are missing in .env: " + ", ".join(missing))

    kwargs = dict(
        account=env("SNOWFLAKE_ACCOUNT"), user=env("SNOWFLAKE_USER"), warehouse=env("SNOWFLAKE_WAREHOUSE"),
        database=env("SNOWFLAKE_DATABASE"), schema=env("SNOWFLAKE_SCHEMA"),
        client_session_keep_alive=True, application="smart-energy-portal",
        session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": int(env("SNOWFLAKE_STATEMENT_TIMEOUT_S", "60")),
                            "QUERY_TAG": "smart-energy-portal"},
    )
    if env("SNOWFLAKE_ROLE"):
        kwargs["role"] = env("SNOWFLAKE_ROLE")
    if env("SNOWFLAKE_PRIVATE_KEY_PATH"):  # key-pair login (recommended for a service account)
        from cryptography.hazmat.primitives import serialization

        key = serialization.load_pem_private_key(
            Path(env("SNOWFLAKE_PRIVATE_KEY_PATH")).read_bytes(),
            password=(env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").encode() or None),
        )
        kwargs["private_key"] = key.private_bytes(
            serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    else:
        kwargs["password"] = env("SNOWFLAKE_PASSWORD")
    return sf.connect(**kwargs)


def _conn(fresh=False):
    con = getattr(_local, "con", None)
    closed = getattr(con, "is_closed", lambda: False)() if con is not None else True
    if fresh or con is None or closed:
        if con is not None:
            try:
                con.close()
            except Exception:  # noqa: BLE001
                pass
        con = _local.con = _connect()
    return con


def run(sql, params=()) -> pd.DataFrame:
    """Execute one statement; lower-case column names; a stale session is retried once."""
    for attempt in (0, 1):
        try:
            cur = _conn(fresh=attempt == 1).cursor()
            try:
                if params:
                    cur.execute(sql, list(params))
                else:
                    cur.execute(sql)
                columns = [str(c[0]).lower() for c in cur.description]
                frame = pd.DataFrame(cur.fetchall(), columns=columns)
            finally:
                cur.close()
            break
        except Exception as error:  # noqa: BLE001
            text = str(error).lower()
            if attempt == 0 and ("session" in text or "token" in text or "closed" in text):
                continue
            raise
    for col in NUMERIC:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")  # Decimal -> float
    return frame


# ------------------------------------------------------------------ what the portal asks for
def readings_for_meter(meter_id) -> pd.DataFrame:
    """15-minute readings of one meter: ts (naive UTC), voltage_v, active_power_kw, reactive_power_kvar, energy_kwh."""
    spec = load_mapping()["readings"]
    cols = ("ts", "voltage_v", "active_power_kw", "reactive_power_kvar", "energy_kwh")
    where = f"{spec['columns']['meter_id']} = ?" + (f" AND ({spec['where']})" if spec.get("where") else "")
    sql = f"SELECT {_select(spec, cols)} FROM {spec['table']} WHERE {where}"
    if spec.get("dedupe_by"):
        sql += f" QUALIFY ROW_NUMBER() OVER (PARTITION BY {spec['dedupe_by']} ORDER BY {spec['columns']['ts']}) = 1"
    sql += f" ORDER BY {spec['columns']['ts']}"
    frame = run(sql, [meter_id])
    # Snowflake TIMESTAMP_NTZ is treated as UTC; zone-aware values are converted to UTC.
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True).dt.tz_localize(None)
    return frame


def meter_info(meter_id) -> dict:
    spec = load_mapping()["dim_meter"]
    cols = tuple(spec["columns"])
    sql = f"SELECT {_select(spec, cols)} FROM {spec['table']} WHERE {spec['columns']['meter_id']} = ? LIMIT 1"
    try:
        frame = run(sql, [meter_id])
    except Exception:  # noqa: BLE001  (the page still works without the details line)
        return {}
    return json.loads(frame.to_json(orient="records"))[0] if not frame.empty else {}


def incidents_for(asset_ids) -> list:
    spec = load_mapping()["incidents"]
    if not spec["table"] or not asset_ids:
        return []  # no incidents table configured (yet)
    cols = tuple(spec["columns"])
    marks = ", ".join("?" for _ in asset_ids)
    sql = (f"SELECT {_select(spec, cols)} FROM {spec['table']} WHERE {spec['columns']['asset_id']} IN ({marks}) "
           f"ORDER BY {spec['columns']['detected_at']} DESC LIMIT 100")
    try:
        frame = run(sql, list(asset_ids))
    except Exception:  # noqa: BLE001  (no incidents table yet: show 'No incidents recorded')
        return []
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def meters_with_readings(limit, wanted=None) -> list:
    """[(meter_id, customer_type)] for meters that really have readings (used to create demo accounts)."""
    m = load_mapping()
    r, d = m["readings"], m["dim_meter"]
    rid, did = r["columns"]["meter_id"], d["columns"]["meter_id"]
    where, params = "", []
    if wanted:
        where = f" WHERE {rid} IN ({', '.join('?' for _ in wanted)})"
        params = list(wanted)
    base = f"SELECT DISTINCT {rid} AS meter_id FROM {r['table']}{where}"
    joined = (f"WITH r AS ({base}) SELECT r.meter_id AS meter_id, COALESCE(d.customer_type, 'unknown') AS customer_type "
              f"FROM r LEFT JOIN (SELECT {did} AS meter_id, {d['columns']['customer_type']} AS customer_type FROM {d['table']}) d "
              f"ON d.meter_id = r.meter_id ORDER BY r.meter_id LIMIT {int(limit)}")
    try:
        frame = run(joined, params)
    except Exception:  # noqa: BLE001  (dim_meter not readable: continue without customer types)
        frame = run(f"{base} ORDER BY meter_id LIMIT {int(limit)}", params).assign(customer_type="unknown")
    return list(frame[["meter_id", "customer_type"]].itertuples(index=False, name=None))

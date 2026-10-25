"""Plan-driven synthetic smart-meter readings for Kafka streaming.

The streaming generator continues the historical dataset: same meters, same
reading formulas, same anomaly families and parameter ranges, same columns,
field order and pandas dtypes. Streaming day 1 is the day right after the last
historical day (read from the historical ``manifest.json``).

Anomalies are planned per streaming day (deterministically from the seed) and
are applied as sustained events, exactly like the historical generator. The
ground truth for the streaming days is written next to the historical answer
key, in the same contract format.
"""

import json
import random
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


READING_COLUMNS = [
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
]

REQUIRED_METER_COLUMNS = {
    "meter_id",
    "transformer_id",
    "customer_type",
}

REQUIRED_TRANSFORMER_COLUMNS = {
    "transformer_id",
    "rated_capacity_kva",
}

PROFILES = {
    "RESIDENTIAL": (0.35, 1.8),
    "COMMERCIAL": (1.2, 4.0),
    "INDUSTRIAL": (3.0, 12.0),
    "AGRICULTURAL": (0.8, 5.0),
    "GOVERNMENT": (0.7, 3.5),
    "PUBLIC_LIGHTING": (0.1, 1.5),
    "OTHER": (0.3, 2.0),
}

INTERVAL_MINUTES = 15
INTERVALS_PER_DAY = 96

# Same offset the historical generator uses for its anomaly plan RNG.
PLAN_SEED_OFFSET = 8107

# The historical generator injects ~10% of transformers worth of events per
# anomaly type over the whole history. Streaming spreads the same density
# evenly over days (10% / history_days per streaming day).
HISTORY_EVENT_FRACTION = 0.10

ANSWER_KEY_FILENAME = "events_stream.jsonl"


# --------------------------------------------------------------------------
# Helpers shared with the streaming driver / consumer
# --------------------------------------------------------------------------

def load_manifest(dataset_dir) -> dict:
    path = Path(dataset_dir) / "manifest.json"
    if not path.is_file():
        raise FileNotFoundError(f"Historical manifest not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def stream_start_from_manifest(dataset_dir) -> datetime:
    """First timestamp the stream must produce: day after the last history day."""
    manifest = load_manifest(dataset_dir)
    start = date.fromisoformat(manifest["start_date"])
    first_stream_day = start + timedelta(days=int(manifest["num_days"]))
    return datetime.combine(first_stream_day, time.min, tzinfo=timezone.utc)


def day_intervals(day_start) -> list:
    """The 96 interval timestamps (UTC) of the day beginning at `day_start`."""
    start = pd.Timestamp(day_start)
    start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
    start = start.floor("D")
    return [
        start + timedelta(minutes=INTERVAL_MINUTES * i)
        for i in range(INTERVALS_PER_DAY)
    ]


def is_last_interval_of_day(timestamp) -> bool:
    """True for the 23:45 interval. Use it to flush and write the day-closed marker."""
    ts = pd.Timestamp(timestamp)
    return ts.hour == 23 and ts.minute == 45


def _iso_z(ts) -> str:
    return pd.Timestamp(ts).isoformat().replace("+00:00", "Z")


def align_reading_schema(readings: pd.DataFrame) -> pd.DataFrame:
    """Return readings in the historical generator's column order/dtypes."""
    missing = set(READING_COLUMNS) - set(readings.columns)
    if missing:
        raise ValueError(f"Readings are missing columns: {sorted(missing)}")

    readings = readings.loc[:, READING_COLUMNS].copy()

    # The historical generator writes schema_version as int32. Keep all
    # measurement columns as float64 and identifiers/timestamps as strings.
    for column in ("reading_id", "event_timestamp", "meter_id", "transformer_id"):
        readings[column] = readings[column].astype("object")

    for column in (
        "voltage_v",
        "active_power_kw",
        "reactive_power_kvar",
        "energy_kwh",
        "power_factor",
    ):
        readings[column] = readings[column].astype("float64")

    readings["schema_version"] = readings["schema_version"].astype("int32")
    return readings


# --------------------------------------------------------------------------
# Simulator
# --------------------------------------------------------------------------

class StreamingSimulator:
    """Generate readings for the days after the historical dataset.

    Normal readings use the same formulas as the historical generator.
    Anomalies are sustained events planned per day and applied while the
    stream runs; their ground truth goes to
    ``<dataset_dir>/answer_key/injected_anomalies/events_stream.jsonl``.
    """

    def __init__(
        self,
        dataset_dir: Path,
        seed: int = 42,
        stream_start=None,
        events_fraction_per_day: float | None = None,
        answer_key_dir: Path | None = None,
        write_answer_key: bool = True,
    ):
        dataset_dir = Path(dataset_dir)
        meter_path = dataset_dir / "dim_meter.parquet"
        transformer_path = dataset_dir / "dim_transformer.parquet"

        if not meter_path.is_file():
            raise FileNotFoundError(f"Meter dimension not found: {meter_path}")
        if not transformer_path.is_file():
            raise FileNotFoundError(
                f"Transformer dimension not found: {transformer_path}"
            )

        self.meters = pd.read_parquet(meter_path)
        transformers = pd.read_parquet(transformer_path)

        missing = REQUIRED_METER_COLUMNS - set(self.meters.columns)
        if missing:
            raise ValueError(
                f"dim_meter.parquet is missing columns: {sorted(missing)}"
            )
        missing = REQUIRED_TRANSFORMER_COLUMNS - set(transformers.columns)
        if missing:
            raise ValueError(
                f"dim_transformer.parquet is missing columns: {sorted(missing)}"
            )

        if self.meters["meter_id"].duplicated().any():
            raise ValueError("Duplicate meter IDs in dimension table.")

        self.meters = self.meters.sort_values("meter_id").reset_index(drop=True)

        # Where the stream begins (UTC midnight) and how dense anomalies are.
        manifest = {}
        if (dataset_dir / "manifest.json").is_file():
            manifest = load_manifest(dataset_dir)

        if stream_start is None:
            if not manifest:
                raise ValueError(
                    "Pass stream_start or provide the historical manifest.json."
                )
            stream_start = stream_start_from_manifest(dataset_dir)
        start = pd.Timestamp(stream_start)
        start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
        self.stream_start = start.floor("D")

        history_days = max(1, int(manifest.get("num_days", 1)))
        if events_fraction_per_day is None:
            events_fraction_per_day = HISTORY_EVENT_FRACTION / history_days
        if not 0.0 <= events_fraction_per_day <= 1.0:
            raise ValueError("events_fraction_per_day must be between 0 and 1.")
        self.events_fraction_per_day = events_fraction_per_day

        self.seed = seed

        self.count = len(self.meters)
        self.meter_ids = self.meters["meter_id"].to_numpy()
        self.transformer_ids = self.meters["transformer_id"].to_numpy()

        profiles = self.meters["customer_type"].map(PROFILES)
        if profiles.isna().any():
            unknown = sorted(
                self.meters.loc[profiles.isna(), "customer_type"].unique()
            )
            raise ValueError(f"Unknown customer types: {unknown}")

        self.base_kw = np.array([p[0] for p in profiles], dtype=float)
        self.peak_kw = np.array([p[1] for p in profiles], dtype=float)

        # Meter positions per transformer (positions == row index after reset).
        self.transformer_rows = {
            str(tid): np.asarray(idx, dtype=int)
            for tid, idx in self.meters.groupby("transformer_id").indices.items()
        }
        self.rated_kva = {
            str(row.transformer_id): float(row.rated_capacity_kva)
            for row in transformers.itertuples(index=False)
        }
        absent = sorted(set(self.transformer_rows) - set(self.rated_kva))
        if absent:
            raise ValueError(
                f"Transformers missing from dim_transformer: {absent[:5]}"
            )

        # reading_id = READ_<meter_id>_<YYYYmmddHHMM>, built from a prefix index.
        self._id_prefix = pd.Index([f"READ_{m}_" for m in self.meter_ids])

        self._plans = {}            # day_index -> list of planned events
        self._contract_events = []  # all planned events, contract format
        self.last_anomaly_events = []

        self._write_answer_key = write_answer_key
        if answer_key_dir is None:
            answer_key_dir = dataset_dir / "answer_key" / "injected_anomalies"
        self.answer_key_path = Path(answer_key_dir) / ANSWER_KEY_FILENAME

    # ----------------------------------------------------------------------
    # Anomaly planning (mirrors historical _make_anomaly_plan, one day at a time)
    # ----------------------------------------------------------------------

    @staticmethod
    def _event_duration(rng: random.Random) -> int:
        """Mix short events with longer, sustained events (same as history)."""
        if rng.random() < 0.5:
            return rng.randint(1, 4)
        return rng.randint(8, 24)

    def _plan_for_day(self, day_index: int) -> list:
        if day_index in self._plans:
            return self._plans[day_index]

        rng = random.Random(self.seed + PLAN_SEED_OFFSET + day_index)
        transformer_ids = sorted(self.transformer_rows)
        event_count = min(
            len(transformer_ids),
            max(1, round(len(transformer_ids) * self.events_fraction_per_day)),
        )

        events = []

        # Transformer overload: distinct transformers, scaled to 105-120% of rating.
        for transformer_id in rng.sample(transformer_ids, event_count):
            duration = self._event_duration(rng)
            events.append({
                "anomaly_type": "TRANSFORMER_OVERLOAD",
                "transformer_id": transformer_id,
                "meter_idx": self.transformer_rows[transformer_id],
                "start_interval": rng.randint(0, INTERVALS_PER_DAY - duration),
                "duration_intervals": duration,
                "target_loading_pct": round(rng.uniform(105.0, 120.0), 2),
                "rated_capacity_kva": self.rated_kva[transformer_id],
            })

        # Voltage drops: a subset of meters on one transformer.
        for _ in range(event_count):
            transformer_id = rng.choice(transformer_ids)
            candidates = self.transformer_rows[transformer_id]
            affected_count = min(len(candidates), rng.randint(1, min(20, len(candidates))))
            affected = np.array(
                sorted(rng.sample([int(i) for i in candidates], affected_count)),
                dtype=int,
            )
            duration = self._event_duration(rng)
            events.append({
                "anomaly_type": "VOLTAGE_DROP",
                "transformer_id": transformer_id,
                "meter_idx": affected,
                "start_interval": rng.randint(0, INTERVALS_PER_DAY - duration),
                "duration_intervals": duration,
                "injected_voltage_v": round(rng.uniform(185.0, 205.0), 2),
            })

        # Irregular consumption: spike or sharp drop at one meter.
        for _ in range(event_count):
            meter_pos = rng.randrange(self.count)
            duration = self._event_duration(rng)
            start_interval = rng.randint(0, INTERVALS_PER_DAY - duration)
            if rng.random() < 0.5:
                multiplier = round(rng.uniform(2.5, 4.0), 3)
                pattern = "SPIKE"
            else:
                multiplier = round(rng.uniform(0.10, 0.25), 3)
                pattern = "SHARP_DROP"
            events.append({
                "anomaly_type": "IRREGULAR_CONSUMPTION",
                "transformer_id": str(self.transformer_ids[meter_pos]),
                "meter_idx": np.array([meter_pos], dtype=int),
                "start_interval": start_interval,
                "duration_intervals": duration,
                "pattern": pattern,
                "power_multiplier": multiplier,
            })

        day_start = self.stream_start + timedelta(days=day_index)
        for number, event in enumerate(events, start=1):
            event["contract_id"] = f"INJ_S{day_index + 1:02d}_{number:04d}"
            self._contract_events.append(self._to_contract_event(event, day_start))

        self._plans[day_index] = events
        self._flush_answer_key()
        return events

    def _to_contract_event(self, event: dict, day_start) -> dict:
        """Same record layout as the historical answer key."""
        anomaly_type = event["anomaly_type"]
        meter_ids = [str(self.meter_ids[i]) for i in event["meter_idx"]]

        if anomaly_type == "IRREGULAR_CONSUMPTION":
            asset_type, asset_id = "METER", meter_ids[0]
        else:
            asset_type, asset_id = "TRANSFORMER", event["transformer_id"]

        duration = event["duration_intervals"]
        start_time = day_start + timedelta(minutes=INTERVAL_MINUTES * event["start_interval"])
        end_time = start_time + timedelta(minutes=INTERVAL_MINUTES * duration)

        scenario_params = {
            "duration_intervals": duration,
            "duration_minutes": duration * INTERVAL_MINUTES,
        }
        for key in (
            "target_loading_pct",
            "rated_capacity_kva",
            "injected_voltage_v",
            "pattern",
            "power_multiplier",
        ):
            if key in event:
                scenario_params[key] = event[key]

        return {
            "event_id": event["contract_id"],
            "anomaly_type": anomaly_type,
            "asset_type": asset_type,
            "asset_id": asset_id,
            "start_time": _iso_z(start_time),
            "end_time": _iso_z(end_time),
            "severity": "HIGH",
            "affected_meters": meter_ids,
            "scenario_params": json.dumps(scenario_params, sort_keys=True),
        }

    def _flush_answer_key(self) -> None:
        """Rewrite the streaming answer key with every day planned so far."""
        if not self._write_answer_key:
            return
        self.answer_key_path.parent.mkdir(parents=True, exist_ok=True)
        with self.answer_key_path.open("w", encoding="utf-8") as f:
            for event in self._contract_events:
                f.write(json.dumps(event) + "\n")

    # ----------------------------------------------------------------------
    # Reading generation
    # ----------------------------------------------------------------------

    def next_interval(self, timestamp, interval_number: int = 0) -> pd.DataFrame:
        """Return one reading per meter for a 15-minute interval.

        `interval_number` is retained for compatibility with existing callers.
        """
        ts = pd.Timestamp(timestamp)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")

        day_index = (ts.floor("D") - self.stream_start).days
        if day_index < 0:
            raise ValueError(
                f"{_iso_z(ts)} is before the stream start "
                f"{_iso_z(self.stream_start)}; it would overlap the historical data."
            )

        interval_in_day = (ts.hour * 60 + ts.minute) // INTERVAL_MINUTES

        # Every reading is a pure function of (seed, timestamp): replaying an
        # interval after a restart yields identical rows, so duplicates are
        # harmless and dedupe on reading_id is lossless.
        rng = np.random.default_rng([self.seed, int(ts.timestamp())])

        timestamp_text = _iso_z(ts)
        timestamp_id = ts.strftime("%Y%m%d%H%M")
        hour = ts.hour
        n = self.count

        evening_factor = (
            1.0 if 17 <= hour <= 22
            else 0.55 if 7 <= hour <= 16
            else 0.25
        )

        # Normal readings: same independent-draw formulas as the historical generator.
        active_kw = np.maximum(
            0.05,
            self.base_kw
            + self.peak_kw * evening_factor * rng.uniform(0.65, 1.0, n),
        )
        reactive_kvar = active_kw * rng.uniform(0.15, 0.40, n)
        energy_kwh = active_kw * 0.25
        apparent_kva = np.sqrt(active_kw**2 + reactive_kvar**2)
        power_factor = active_kw / apparent_kva
        voltage_v = rng.normal(220.0, 2.0, n)

        # Anomalies planned for this day that are active in this interval.
        active_events = [
            e for e in self._plan_for_day(day_index)
            if e["start_interval"] <= interval_in_day
            < e["start_interval"] + e["duration_intervals"]
        ]
        self.last_anomaly_events = []

        # Meter-level anomalies first (as in the historical pipeline) ...
        for event in active_events:
            idx = event["meter_idx"]
            if event["anomaly_type"] == "VOLTAGE_DROP":
                voltage_v[idx] = event["injected_voltage_v"]
            elif event["anomaly_type"] == "IRREGULAR_CONSUMPTION":
                multiplier = event["power_multiplier"]
                active_kw[idx] *= multiplier
                reactive_kvar[idx] *= multiplier
                energy_kwh[idx] *= multiplier

        # ... then transformer overload, scaled from the resulting aggregate load.
        for event in active_events:
            if event["anomaly_type"] != "TRANSFORMER_OVERLOAD":
                continue
            idx = event["meter_idx"]
            current_kva = float(np.hypot(active_kw[idx].sum(), reactive_kvar[idx].sum()))
            if current_kva <= 0:
                continue
            target_kva = event["rated_capacity_kva"] * event["target_loading_pct"] / 100.0
            factor = target_kva / current_kva
            active_kw[idx] *= factor
            reactive_kvar[idx] *= factor
            energy_kwh[idx] *= factor

        for event in active_events:
            self.last_anomaly_events.append({
                "event_id": event["contract_id"],
                "anomaly_type": event["anomaly_type"],
                "transformer_id": event["transformer_id"],
                "event_timestamp": timestamp_text,
                "affected_meter_count": int(len(event["meter_idx"])),
            })

        readings = pd.DataFrame({
            "reading_id": (self._id_prefix + timestamp_id).to_numpy(dtype=object),
            "event_timestamp": timestamp_text,
            "meter_id": self.meter_ids,
            "transformer_id": self.transformer_ids,
            "voltage_v": np.round(voltage_v, 2),
            "active_power_kw": np.round(active_kw, 4),
            "reactive_power_kvar": np.round(reactive_kvar, 4),
            "energy_kwh": np.round(energy_kwh, 4),
            "power_factor": np.round(power_factor, 4),
            "schema_version": np.full(n, 1, dtype=np.int32),
        })
        return align_reading_schema(readings)
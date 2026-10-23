
"""Stateful synthetic smart-meter readings for Kafka streaming."""

from pathlib import Path

import numpy as np
import pandas as pd


REQUIRED_METER_COLUMNS = {
    "meter_id",
    "transformer_id",
    "customer_type",
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


class StreamingSimulator:
    """Generate evolving readings while preserving the existing grid IDs."""

    def __init__(self, dataset_dir: Path, seed: int = 42):
        dataset_dir = Path(dataset_dir)
        meter_path = dataset_dir / "dim_meter.parquet"

        if not meter_path.is_file():
            raise FileNotFoundError(
                f"Meter dimension not found: {meter_path}"
            )

        self.meters = pd.read_parquet(meter_path)

        missing = REQUIRED_METER_COLUMNS - set(self.meters.columns)
        if missing:
            raise ValueError(
                f"dim_meter.parquet is missing columns: {sorted(missing)}"
            )

        if self.meters["meter_id"].duplicated().any():
            raise ValueError("Duplicate meter IDs in dimension table.")

        self.meters = self.meters.sort_values(
            "meter_id"
        ).reset_index(drop=True)

        self.rng = np.random.default_rng(seed)
        self.count = len(self.meters)

        self.meter_ids = self.meters["meter_id"].to_numpy()
        self.transformer_ids = self.meters[
            "transformer_id"
        ].to_numpy()

        profiles = self.meters["customer_type"].map(PROFILES)
        if profiles.isna().any():
            unknown = sorted(
                self.meters.loc[
                    profiles.isna(), "customer_type"
                ].unique()
            )
            raise ValueError(f"Unknown customer types: {unknown}")

        self.base_kw = np.array([p[0] for p in profiles], dtype=float)
        self.peak_kw = np.array([p[1] for p in profiles], dtype=float)

        # Preserve state between consecutive intervals.
        self.previous_kw = self.base_kw.copy()
        self.previous_voltage = self.rng.normal(
            220.0, 1.0, size=self.count
        )

    def next_interval(
        self, timestamp, interval_number: int
    ) -> pd.DataFrame:
        """Return one fresh reading per meter for a 15-minute interval."""

        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        hour = ts.hour

        # Simplified time-of-day demand pattern.
        demand_factor = (
            1.0 if 17 <= hour <= 22
            else 0.55 if 7 <= hour <= 16
            else 0.25
        )

        target_kw = self.base_kw + (
            self.peak_kw
            * demand_factor
            * self.rng.uniform(0.65, 1.0, self.count)
        )

        # Smooth changes: current demand depends on the previous interval.
        noise = self.rng.normal(
            0.0, np.maximum(target_kw * 0.05, 0.03), self.count
        )
        active_kw = np.maximum(
            0.05,
            0.65 * self.previous_kw + 0.35 * target_kw + noise,
        )
        self.previous_kw = active_kw.copy()

        reactive_kvar = active_kw * self.rng.uniform(
            0.15, 0.40, self.count
        )

        apparent_kva = np.sqrt(
            active_kw**2 + reactive_kvar**2
        )
        power_factor = np.divide(
            active_kw,
            apparent_kva,
            out=np.ones_like(active_kw),
            where=apparent_kva > 0,
        )

        # Voltage moves gradually rather than being independently random.
        voltage_noise = self.rng.normal(0.0, 0.8, self.count)
        voltage_v = (
            self.previous_voltage
            + 0.20 * (220.0 - self.previous_voltage)
            + voltage_noise
        )
        voltage_v = np.clip(voltage_v, 200.0, 240.0)
        self.previous_voltage = voltage_v.copy()

        return pd.DataFrame({
            "reading_id": [
                f"sim-{interval_number:06d}-{meter_id}"
                for meter_id in self.meter_ids
            ],
            "event_timestamp": ts.isoformat().replace("+00:00", "Z"),
            "meter_id": self.meter_ids,
            "transformer_id": self.transformer_ids,
            "voltage_v": np.round(voltage_v, 2),
            "active_power_kw": np.round(active_kw, 4),
            "reactive_power_kvar": np.round(reactive_kvar, 4),
            "energy_kwh": np.round(active_kw * 0.25, 4),
            "power_factor": np.round(power_factor, 4),
            "schema_version": 1,
        })

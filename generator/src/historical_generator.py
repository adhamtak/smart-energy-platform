import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import argparse
import json
import math
import random
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from collections import defaultdict
import pandas as pd

from config import GeneratorConfig


STANDARD_TRANSFORMER_KVA = [25, 50, 100, 160, 250, 400, 630, 1000]


def _rated_capacity_kva(meters_per_transformer):
    """Pick a standard transformer size for the meters it serves.

    Normal load is roughly 2 kVA per meter; size for about 60% loading so an
    injected overload (105-120%) is a realistic multiple of normal load and not
    a 25x spike on every meter.
    """
    needed = meters_per_transformer * 2.0 / 0.6
    for size in STANDARD_TRANSFORMER_KVA:
        if size >= needed:
            return float(size)
    return float(math.ceil(needed / 250) * 250)


def generate_dimensions(config):
    """Generate synthetic dimensions matching Data Contract v1."""
    rng = random.Random(config.seed)

    regions = [
        {"region_id": "NCAI", "region_name": "North Cairo"},
        {"region_id": "SCAI", "region_name": "South Cairo"},
        {"region_id": "ALX", "region_name": "Alexandria"},
        {"region_id": "CNL", "region_name": "Canal"},
        {"region_id": "NDLT", "region_name": "North Delta"},
        {"region_id": "SDLT", "region_name": "South Delta"},
        {"region_id": "BEH", "region_name": "Beheira"},
        {"region_id": "MEG", "region_name": "Middle Egypt"},
        {"region_id": "UEG", "region_name": "Upper Egypt"},
    ]

    num_meters = config.num_meters
    meters_per_transformer = config.meters_per_transformer

    if num_meters < 1:
        raise ValueError("num_meters must be at least 1")
    if meters_per_transformer < 1:
        raise ValueError("meters_per_transformer must be at least 1")

    num_transformers = math.ceil(
        num_meters / meters_per_transformer
    )

    # Engineering assumption: approximately 22 transformers per substation.
    num_substations = max(1, math.ceil(num_transformers / 22))

    substations = []
    transformers = []
    meters = []

    # Create substations and assign each to a region.
    for i in range(num_substations):
        region = regions[i % len(regions)]
        region_id = region["region_id"]

        substations.append({
            "substation_id": f"SUB_{region_id}_{i + 1:02d}",
            "name": f"{region['region_name']} Substation {i + 1:02d}",
            "region_id": region_id,
            "city": region["region_name"],
            "primary_kv": 66.0,
            "secondary_kv": 11.0,
            "capacity_mva": 20.0,
            "status": "ACTIVE",
        })

    # Assign transformers to substations.
    for i in range(num_transformers):
        substation = substations[i % num_substations]
        region_id = substation["region_id"]

        transformers.append({
            "transformer_id": f"TX_{region_id}_{i + 1:04d}",
            "substation_id": substation["substation_id"],
            "region_id": region_id,
            "rated_capacity_kva": _rated_capacity_kva(meters_per_transformer),
            "nominal_voltage_v": 380.0,
            "install_date": date(2020, 1, 1),
            "status": "ACTIVE",
        })

    # Synthetic proportions, not official EEHC subscriber proportions.
    customer_categories = [
        ("RESIDENTIAL", 0.70),
        ("COMMERCIAL", 0.12),
        ("INDUSTRIAL", 0.04),
        ("AGRICULTURAL", 0.06),
        ("GOVERNMENT", 0.03),
        ("PUBLIC_LIGHTING", 0.03),
        ("OTHER", 0.02),
    ]

    categories = [item[0] for item in customer_categories]
    weights = [item[1] for item in customer_categories]

    for i in range(num_meters):
        transformer = transformers[i // meters_per_transformer]

        meters.append({
            "meter_id": f"MTR{i + 1:06d}",
            "transformer_id": transformer["transformer_id"],
            "customer_type": rng.choices(
                categories, weights=weights, k=1
            )[0],
            "tariff_category": None,
            "phase_type": rng.choices(
                ["SINGLE_PHASE", "THREE_PHASE"],
                weights=[0.80, 0.20],
                k=1,
            )[0],
            "install_date": date(2022, 1, 1),
            "status": "ACTIVE",
        })

    # Preserve the interface expected by your existing main().
    return regions, substations, transformers, meters

def generate_reading_batch(config, meters, rng):
    """Generate readings for one batch of meters (vectorized).

    Same formulas as before (independent draws per meter and interval); returns
    a DataFrame with the contract columns and dtypes.
    """
    profiles = {
        "RESIDENTIAL": (0.35, 1.8),
        "COMMERCIAL": (1.2, 4.0),
        "INDUSTRIAL": (3.0, 12.0),
        "AGRICULTURAL": (0.8, 5.0),
        "GOVERNMENT": (0.7, 3.5),
        "PUBLIC_LIGHTING": (0.1, 1.5),
        "OTHER": (0.3, 2.0),
    }

    n_intervals = config.num_days * 96
    n_meters = len(meters)
    start = datetime.combine(config.start_date, time.min, tzinfo=timezone.utc)

    stamps = [start + timedelta(minutes=15 * i) for i in range(n_intervals)]
    ts_text = np.array(
        [t.isoformat().replace("+00:00", "Z") for t in stamps], dtype=object
    )
    ts_id = np.array([t.strftime("%Y%m%d%H%M") for t in stamps], dtype=object)
    hours = np.array([t.hour for t in stamps])
    evening_factor = np.where(
        (hours >= 17) & (hours <= 22),
        1.0,
        np.where((hours >= 7) & (hours <= 16), 0.55, 0.25),
    )

    base_kw = np.array([profiles[m["customer_type"]][0] for m in meters])
    peak_kw = np.array([profiles[m["customer_type"]][1] for m in meters])

    nprng = np.random.default_rng(rng.getrandbits(64))
    shape = (n_meters, n_intervals)

    active = np.maximum(
        0.05,
        base_kw[:, None]
        + peak_kw[:, None] * evening_factor[None, :] * nprng.uniform(0.65, 1.0, shape),
    )
    reactive = active * nprng.uniform(0.15, 0.4, shape)
    energy = active * 0.25
    power_factor = active / np.sqrt(active**2 + reactive**2)
    voltage = nprng.normal(220.0, 2.0, shape)

    prefixes = np.array([f"READ_{m['meter_id']}_" for m in meters], dtype=object)
    reading_ids = np.add.outer(prefixes, ts_id).ravel()

    df = pd.DataFrame({
        "reading_id": reading_ids,
        "event_timestamp": np.tile(ts_text, n_meters),
        "meter_id": np.repeat(
            np.array([m["meter_id"] for m in meters], dtype=object), n_intervals
        ),
        "transformer_id": np.repeat(
            np.array([m["transformer_id"] for m in meters], dtype=object),
            n_intervals,
        ),
        "voltage_v": np.round(voltage.ravel(), 2),
        "active_power_kw": np.round(active.ravel(), 4),
        "reactive_power_kvar": np.round(reactive.ravel(), 4),
        "energy_kwh": np.round(energy.ravel(), 4),
        "power_factor": np.round(power_factor.ravel(), 4),
        "schema_version": np.full(n_meters * n_intervals, 1, dtype=np.int32),
    })
    return df


def write_dimensions(output_dir, dimensions):
    """Write dimension tables using explicit Parquet schemas."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    schemas = {
        "dim_region": pa.schema([
            ("region_id", pa.string()),
            ("region_name", pa.string()),
        ]),
        "dim_substation": pa.schema([
            ("substation_id", pa.string()),
            ("name", pa.string()),
            ("region_id", pa.string()),
            ("city", pa.string()),
            ("primary_kv", pa.float64()),
            ("secondary_kv", pa.float64()),
            ("capacity_mva", pa.float64()),
            ("status", pa.string()),
        ]),
        "dim_transformer": pa.schema([
            ("transformer_id", pa.string()),
            ("substation_id", pa.string()),
            ("region_id", pa.string()),
            ("rated_capacity_kva", pa.float64()),
            ("nominal_voltage_v", pa.float64()),
            ("install_date", pa.date32()),
            ("status", pa.string()),
        ]),
        "dim_meter": pa.schema([
            ("meter_id", pa.string()),
            ("transformer_id", pa.string()),
            ("customer_type", pa.string()),
            ("tariff_category", pa.string()),
            ("phase_type", pa.string()),
            ("install_date", pa.date32()),
            ("status", pa.string()),
        ]),
    }

    for table_name, rows in dimensions.items():
        table = pa.Table.from_pylist(
            rows,
            schema=schemas[table_name],
        )

        path = output_dir / f"{table_name}.parquet"
        pq.write_table(table, path, compression="snappy")

        print(
            f"Wrote {table_name}: {table.num_rows:,} rows -> {path}"
        )

def write_manifest(output_dir, config, tables, total_readings, batch_size):
    manifest = {
        "data_type": "synthetic",
        "seed": config.seed,
        "start_date": config.start_date.isoformat(),
        "num_days": config.num_days,
        "num_meters": config.num_meters,
        "interval_minutes": 15,
        "readings_per_meter_per_day": 96,
        "batch_meters": batch_size,
        "records": {
            **{name: len(rows) for name, rows in tables.items()},
            "readings": total_readings,
        },
    }

    path = output_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote manifest -> {path}")



def _timestamp_at(config, interval_index):
    """Return a UTC ISO timestamp matching the generator's 15-minute intervals."""
    start = datetime.combine(
        config.start_date,
        time.min,
        tzinfo=timezone.utc,
    )
    ts = start + timedelta(minutes=15 * interval_index)
    return ts.isoformat().replace("+00:00", "Z")


def _event_duration(rng, total_intervals):
    """Mix short events with longer, sustained events."""
    if rng.random() < 0.5:
        return rng.randint(1, min(4, total_intervals))

    lower = min(8, total_intervals)
    upper = min(24, total_intervals)
    return rng.randint(lower, upper)


def _make_anomaly_plan(config, dimensions):
    """
    Build a deterministic injection plan.
    This plan is ground truth, not an input to anomaly detection.
    """
    _, _, transformers, meters = dimensions
    rng = random.Random(config.seed + 8107)

    if not transformers or not meters:
        raise ValueError("Cannot inject anomalies without transformers and meters.")

    total_intervals = config.num_days * 96
    event_count = min(
        len(transformers),
        max(1, round(len(transformers) * 0.10)),
    )

    transformer_ids = [t["transformer_id"] for t in transformers]
    rated_capacity = {
        t["transformer_id"]: float(t["rated_capacity_kva"])
        for t in transformers
    }

    meters_by_transformer = defaultdict(list)
    for meter in meters:
        meters_by_transformer[meter["transformer_id"]].append(
            meter["meter_id"]
        )

    events = []

    # Transformer overload: use distinct transformers for these events.
    overload_transformers = rng.sample(transformer_ids, event_count)

    for i, transformer_id in enumerate(overload_transformers):
        duration = _event_duration(rng, total_intervals)
        start_idx = rng.randint(0, total_intervals - duration)
        target_pct = round(rng.uniform(105.0, 120.0), 2)

        events.append({
            "event_id": f"ANOM_OVERLOAD_{i + 1:05d}",
            "anomaly_type": "TRANSFORMER_OVERLOAD",
            "transformer_id": transformer_id,
            "meter_ids": meters_by_transformer[transformer_id],
            "start_idx": start_idx,
            "duration_intervals": duration,
            "target_loading_pct": target_pct,
            "rated_capacity_kva": rated_capacity[transformer_id],
        })

    # Voltage drops: affect a subset of meters on a transformer.
    for i in range(event_count):
        transformer_id = rng.choice(transformer_ids)
        candidates = meters_by_transformer[transformer_id]
        affected_count = min(len(candidates), rng.randint(1, min(20, len(candidates))))
        affected_meters = rng.sample(candidates, affected_count)

        duration = _event_duration(rng, total_intervals)
        start_idx = rng.randint(0, total_intervals - duration)

        events.append({
            "event_id": f"ANOM_VOLTAGE_{i + 1:05d}",
            "anomaly_type": "VOLTAGE_DROP",
            "transformer_id": transformer_id,
            "meter_ids": affected_meters,
            "start_idx": start_idx,
            "duration_intervals": duration,
            "injected_voltage_v": round(rng.uniform(185.0, 205.0), 2),
        })

    # Irregular consumption: a sudden spike or a sharp drop at a meter.
    all_meter_ids = [m["meter_id"] for m in meters]
    meter_lookup = {m["meter_id"]: m for m in meters}

    for i in range(event_count):
        meter_id = rng.choice(all_meter_ids)
        meter = meter_lookup[meter_id]

        duration = _event_duration(rng, total_intervals)
        start_idx = rng.randint(0, total_intervals - duration)

        if rng.random() < 0.5:
            multiplier = round(rng.uniform(2.5, 4.0), 3)
            pattern = "SPIKE"
        else:
            multiplier = round(rng.uniform(0.10, 0.25), 3)
            pattern = "SHARP_DROP"

        events.append({
            "event_id": f"ANOM_CONSUMPTION_{i + 1:05d}",
            "anomaly_type": "IRREGULAR_CONSUMPTION",
            "transformer_id": meter["transformer_id"],
            "meter_ids": [meter_id],
            "start_idx": start_idx,
            "duration_intervals": duration,
            "pattern": pattern,
            "power_multiplier": multiplier,
        })

    # Convert interval indexes to explicit UTC boundaries for the answer key.
    for event in events:
        start_idx = event.pop("start_idx")
        duration = event["duration_intervals"]
        event["start_timestamp"] = _timestamp_at(config, start_idx)
        event["end_timestamp_exclusive"] = _timestamp_at(
            config, start_idx + duration
        )
        event["duration_minutes"] = duration * 15

    return events


def _write_parquet_preserving_schema(path, df, original_schema):
    """Rewrite a batch while retaining its original Arrow schema."""
    table = pa.Table.from_pandas(
        df,
        schema=original_schema,
        preserve_index=False,
        safe=True,
    )
    pq.write_table(table, path, compression="snappy")


def _apply_meter_anomalies(reading_files, events):
    """Inject voltage drops and irregular consumption into selected readings."""
    meter_events = [
        event for event in events
        if event["anomaly_type"] in {
            "VOLTAGE_DROP",
            "IRREGULAR_CONSUMPTION",
        }
    ]

    for path in reading_files:
        table = pq.read_table(path)
        df = table.to_pandas()
        batch_meter_ids = set(df["meter_id"].unique())

        for event in meter_events:
            affected = batch_meter_ids.intersection(event["meter_ids"])
            if not affected:
                continue

            timestamps = set()
            # Event timestamps are interval starts, end boundary is exclusive.
            for interval in range(
                event["duration_minutes"] // 15
            ):
                start = datetime.fromisoformat(
                    event["start_timestamp"].replace("Z", "+00:00")
                )
                ts = start + timedelta(minutes=15 * interval)
                timestamps.add(ts.isoformat().replace("+00:00", "Z"))

            mask = (
                df["meter_id"].isin(affected)
                & df["event_timestamp"].isin(timestamps)
            )

            if event["anomaly_type"] == "VOLTAGE_DROP":
                df.loc[mask, "voltage_v"] = event["injected_voltage_v"]

            else:
                multiplier = event["power_multiplier"]

                for column in (
                    "active_power_kw",
                    "reactive_power_kvar",
                    "energy_kwh",
                ):
                    df.loc[mask, column] = (
                        df.loc[mask, column] * multiplier
                    ).round(4)

        _write_parquet_preserving_schema(path, df, table.schema)


def _calculate_overload_scales(reading_files, events):
    """
    Calculate a scale factor per transformer and interval.
    Aggregate active/reactive power across all connected meters first.
    """
    overload_events = [
        event for event in events
        if event["anomaly_type"] == "TRANSFORMER_OVERLOAD"
    ]

    if not overload_events:
        return {}

    aggregate_frames = []

    for path in reading_files:
        df = pq.read_table(
            path,
            columns=[
                "transformer_id",
                "event_timestamp",
                "active_power_kw",
                "reactive_power_kvar",
            ],
        ).to_pandas()

        grouped = (
            df.groupby(["transformer_id", "event_timestamp"], as_index=False)
            .agg(
                total_kw=("active_power_kw", "sum"),
                total_kvar=("reactive_power_kvar", "sum"),
            )
        )
        aggregate_frames.append(grouped)

    aggregate = pd.concat(aggregate_frames, ignore_index=True)
    aggregate = (
        aggregate.groupby(
            ["transformer_id", "event_timestamp"], as_index=False
        )
        .agg(
            total_kw=("total_kw", "sum"),
            total_kvar=("total_kvar", "sum"),
        )
    )

    aggregate["current_kva"] = (
        aggregate["total_kw"] ** 2 + aggregate["total_kvar"] ** 2
    ) ** 0.5

    scales = {}
    scale_values_by_event = defaultdict(list)

    for event in overload_events:
        start = datetime.fromisoformat(
            event["start_timestamp"].replace("Z", "+00:00")
        )
        target_kva = (
            event["rated_capacity_kva"]
            * event["target_loading_pct"] / 100.0
        )

        for interval in range(event["duration_minutes"] // 15):
            ts = start + timedelta(minutes=15 * interval)
            timestamp = ts.isoformat().replace("+00:00", "Z")

            matches = aggregate[
                (aggregate["transformer_id"] == event["transformer_id"])
                & (aggregate["event_timestamp"] == timestamp)
            ]

            if matches.empty:
                continue

            current_kva = float(matches.iloc[0]["current_kva"])
            if current_kva <= 0:
                continue

            factor = target_kva / current_kva
            scales[(event["transformer_id"], timestamp)] = factor
            scale_values_by_event[event["event_id"]].append(factor)

    for event in overload_events:
        factors = scale_values_by_event[event["event_id"]]
        if factors:
            event["scale_factor_min"] = round(min(factors), 6)
            event["scale_factor_max"] = round(max(factors), 6)

    return scales


def _apply_overload_scales(reading_files, scales):
    """Scale all meters on each affected transformer/interval."""
    if not scales:
        return

    scale_df = pd.DataFrame(
        [
            {
                "transformer_id": transformer_id,
                "event_timestamp": timestamp,
                "_injection_factor": factor,
            }
            for (transformer_id, timestamp), factor in scales.items()
        ]
    )

    for path in reading_files:
        table = pq.read_table(path)
        df = table.to_pandas()

        df = df.merge(
            scale_df,
            on=["transformer_id", "event_timestamp"],
            how="left",
            sort=False,
        )
        df["_injection_factor"] = df["_injection_factor"].fillna(1.0)

        affected = df["_injection_factor"] != 1.0
        for column in (
            "active_power_kw",
            "reactive_power_kvar",
            "energy_kwh",
        ):
            df.loc[affected, column] = (
                df.loc[affected, column]
                * df.loc[affected, "_injection_factor"]
            ).round(4)

        df.drop(columns=["_injection_factor"], inplace=True)
        _write_parquet_preserving_schema(path, df, table.schema)


import json


def _to_contract_event(event: dict, sequence: int) -> dict:
    """Convert an internal anomaly event to the Section 7 contract."""

    anomaly_type = event["anomaly_type"]

    if anomaly_type == "TRANSFORMER_OVERLOAD":
        asset_type = "TRANSFORMER"
        asset_id = event["transformer_id"]

    elif anomaly_type == "VOLTAGE_DROP":
        # One event affecting selected meters on a transformer.
        asset_type = "TRANSFORMER"
        asset_id = event["transformer_id"]

    elif anomaly_type == "IRREGULAR_CONSUMPTION":
        asset_type = "METER"
        asset_id = event["meter_ids"][0]

    else:
        raise ValueError(f"Unsupported anomaly type: {anomaly_type}")

    # Provisional default until the team agrees on severity thresholds.
    severity = event.get("severity", "HIGH")

    # Keep event-specific parameters for evaluation and debugging.
    common_fields = {
        "event_id",
        "anomaly_type",
        "transformer_id",
        "meter_ids",
        "start_timestamp",
        "end_timestamp_exclusive",
        "severity",
    }

    scenario_params = {
        key: value
        for key, value in event.items()
        if key not in common_fields
    }

    return {
        "event_id": f"INJ_{sequence:04d}",
        "anomaly_type": anomaly_type,
        "asset_type": asset_type,
        "asset_id": asset_id,
        "start_time": event["start_timestamp"],
        "end_time": event["end_timestamp_exclusive"],
        "severity": severity,
        "affected_meters": event["meter_ids"],
        "scenario_params": json.dumps(
            scenario_params,
            sort_keys=True,
        ),
    }


def inject_anomalies(output_dir, config, dimensions):
    """Inject anomalies after normal generation and save separate ground truth."""
    readings_dir = Path(output_dir) / "readings"
    reading_files = sorted(readings_dir.glob("part-*.parquet"))

    if not reading_files:
        raise FileNotFoundError(
            f"No generated reading batches found in {readings_dir}"
        )

    events = _make_anomaly_plan(config, dimensions)

    # First inject meter-level anomalies.
    _apply_meter_anomalies(reading_files, events)

    # Then calculate transformer overload using the resulting readings.
    scales = _calculate_overload_scales(reading_files, events)
    _apply_overload_scales(reading_files, scales)

    # Keep answer key separate from the data consumed by detection jobs.
    answer_key_dir = (
        Path(output_dir) / "answer_key" / "injected_anomalies"
    )
    answer_key_dir.mkdir(parents=True, exist_ok=True)


    events_path = answer_key_dir / "events.jsonl"

    with events_path.open("w", encoding="utf-8") as f:
        for sequence, event in enumerate(events, start=1):
            contract_event = _to_contract_event(event, sequence)
            f.write(json.dumps(contract_event) + "\n")

    print(f"Ground-truth answer key: {events_path}")


    summary = defaultdict(int)
    for event in events:
        summary[event["anomaly_type"]] += 1

    print("Anomaly injection complete.")
    print(f"Ground truth: {events_path}")
    print(f"Injected events: {len(events)}")
    for anomaly_type, count in sorted(summary.items()):
        print(f"  {anomaly_type}: {count}")

    return events



def write_partitioned_by_date(output_dir):
    """Re-write the final readings as Hive-style date partitions.

    Layout: <output_dir>/readings_by_date/dt=YYYY-MM-DD/part-XXXXX.parquet
    This is the same layout the streaming consumer must use, so history and the
    stream land side by side and a closed streaming day is just another partition.
    `dt` is derived from the reading's own event_timestamp, never wall-clock time.
    """
    output_dir = Path(output_dir)
    src_files = sorted((output_dir / "readings").glob("part-*.parquet"))
    dst_root = output_dir / "readings_by_date"
    if dst_root.exists():
        for old in dst_root.rglob("*.parquet"):
            old.unlink()

    days = set()
    for batch_number, path in enumerate(src_files):
        table = pq.read_table(path)
        dt = pd.Series(table.column("event_timestamp").to_pandas().str[:10])
        for day in sorted(dt.unique()):
            mask = pa.array((dt == day).to_numpy())
            part_dir = dst_root / f"dt={day}"
            part_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                table.filter(mask),
                part_dir / f"part-{batch_number:05d}.parquet",
                compression="snappy",
            )
            days.add(day)

    print(f"Wrote date-partitioned readings ({len(days)} days) -> {dst_root}")
    return sorted(days)


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic smart-meter historical data in chunks."
    )
    parser.add_argument("--meters", type=int, default=None)
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--start-date", type=date.fromisoformat, default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/generated"),
    )
    parser.add_argument(
        "--batch-meters",
        type=int,
        default=250,
        help="Number of meters held in memory per reading batch.",
    )

    parser.add_argument(
        "--skip-partition",
        action="store_true",
        help="Do not write the readings_by_date/dt=YYYY-MM-DD layout.",
    )

    args = parser.parse_args()

    if args.batch_meters < 1:
        parser.error("--batch-meters must be at least 1")

    config = GeneratorConfig.from_env()

    overrides = {}
    if args.meters is not None:
        overrides["num_meters"] = args.meters
    if args.days is not None:
        overrides["num_days"] = args.days
    if args.start_date is not None:
        overrides["start_date"] = args.start_date

    config = replace(config, **overrides)
    config.validate()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    regions, substations, transformers, meters = generate_dimensions(config)

    dimension_tables = {
        "dim_region": regions,
        "dim_substation": substations,
        "dim_transformer": transformers,
        "dim_meter": meters,
    }

    write_dimensions(output_dir, dimension_tables)

    # Generate normal readings in bounded batches.
    readings_dir = output_dir / "readings"
    readings_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(config.seed)
    total_readings = 0
    batch_number = 0

    for offset in range(0, len(meters), args.batch_meters):
        meter_batch = meters[offset:offset + args.batch_meters]

        df = generate_reading_batch(config, meter_batch, rng)

        path = readings_dir / f"part-{batch_number:05d}.parquet"
        df.to_parquet(path, index=False, compression="snappy")

        total_readings += len(df)
        batch_number += 1

        print(
            f"Batch {batch_number}: "
            f"{len(meter_batch):,} meters, "
            f"{len(df):,} readings "
            f"(total: {total_readings:,})"
        )

        # Release this batch before generating the next one.
        del df

    # Inject anomalies only after every normal reading batch exists.
    # The function also writes the separate ground-truth answer key.
    anomaly_events = inject_anomalies(
        output_dir=output_dir,
        config=config,
        dimensions=(
            regions,
            substations,
            transformers,
            meters,
        ),
    )

    # Date-partitioned copy (shared layout with the streaming consumer).
    if not args.skip_partition:
        write_partitioned_by_date(output_dir)

    # Write the manifest after injection so it can describe the final dataset.
    write_manifest(
        output_dir,
        config,
        dimension_tables,
        total_readings,
        args.batch_meters,
    )

    expected_readings = config.num_meters * config.num_days * 96

    print("\nGeneration complete.")
    print(f"Meters: {len(meters):,}")
    print(f"Expected readings: {expected_readings:,}")
    print(f"Actual readings: {total_readings:,}")
    print(f"Parquet batch files: {batch_number:,}")
    print(f"Injected anomaly events: {len(anomaly_events):,}")
    print(f"Output directory: {output_dir.resolve()}")

    print(
        "Ground truth: "
        f"{(output_dir / 'answer_key' / 'injected_anomalies' / 'events.jsonl').resolve()}"
    )

    if total_readings != expected_readings:
        raise RuntimeError(
            f"Reading count mismatch: expected {expected_readings}, "
            f"generated {total_readings}"
        )


if __name__ == "__main__":
    main()
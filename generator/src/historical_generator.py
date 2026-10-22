
import argparse
import json
import math
import random
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd

from config import GeneratorConfig


def generate_dimensions(config: GeneratorConfig):
    """Generate consistent synthetic grid dimension tables."""
    rng = random.Random(config.seed)

    regions = [{
        "region_id": "NCAI",
        "region_name": "North Cairo",
    }]

    num_transformers = math.ceil(
        config.num_meters / config.meters_per_transformer
    )

    substations = []
    transformers = []
    meters = []

    for i in range(num_transformers):
        substation_number = i // 10 + 1
        substation_id = f"SUB_NCAI_{substation_number:02d}"
        transformer_id = f"TX_NCAI_{i + 1:04d}"

        # Create each substation once, rather than searching the list.
        if i % 10 == 0:
            substations.append({
                "substation_id": substation_id,
                "name": f"North Cairo Substation {substation_number:02d}",
                "region_id": "NCAI",
                "city": "Cairo",
                "primary_kv": 66.0,
                "secondary_kv": 11.0,
                "capacity_mva": 10.0,
                "status": "ACTIVE",
            })

        transformers.append({
            "transformer_id": transformer_id,
            "substation_id": substation_id,
            "region_id": "NCAI",
            "rated_capacity_kva": 500.0,
            "nominal_voltage_v": 380.0,
            "install_date": config.start_date,
            "status": "ACTIVE",
        })

        meters_remaining = config.num_meters - len(meters)
        meters_for_transformer = min(
            config.meters_per_transformer,
            meters_remaining,
        )

        for _ in range(meters_for_transformer):
            meter_number = len(meters) + 1

            customer_type = rng.choices(
                ["RESIDENTIAL", "COMMERCIAL", "INDUSTRIAL"],
                weights=[0.80, 0.17, 0.03],
                k=1,
            )[0]

            meters.append({
                "meter_id": f"MTR{meter_number:06d}",
                "transformer_id": transformer_id,
                "customer_type": customer_type,
                "tariff_category": None,
                "phase_type": (
                    "THREE_PHASE"
                    if customer_type == "INDUSTRIAL"
                    else "SINGLE_PHASE"
                ),
                "install_date": config.start_date,
                "status": "ACTIVE",
            })

    return regions, substations, transformers, meters


def generate_reading_batch(config, meters, rng):
    """Generate readings for one batch of meters."""
    readings = []

    start = datetime.combine(
        config.start_date,
        time.min,
        tzinfo=timezone.utc,
    )

    profiles = {
        "RESIDENTIAL": (0.35, 1.8),
        "COMMERCIAL": (1.2, 4.0),
        "INDUSTRIAL": (3.0, 12.0),
    }

    for meter in meters:
        base_kw, peak_kw = profiles[meter["customer_type"]]

        for interval in range(config.num_days * 96):
            event_time = start + timedelta(minutes=15 * interval)
            hour = event_time.hour

            evening_factor = (
                1.0 if 17 <= hour <= 22
                else 0.55 if 7 <= hour <= 16
                else 0.25
            )

            active_power_kw = max(
                0.05,
                base_kw
                + peak_kw * evening_factor * rng.uniform(0.65, 1.0),
            )

            reactive_power_kvar = (
                active_power_kw * rng.uniform(0.15, 0.4)
            )
            energy_kwh = active_power_kw * 0.25

            apparent_power_kva = math.sqrt(
                active_power_kw**2 + reactive_power_kvar**2
            )
            power_factor = active_power_kw / apparent_power_kva

            readings.append({
                "reading_id": (
                    f"READ_{meter['meter_id']}_"
                    f"{event_time.strftime('%Y%m%d%H%M')}"
                ),
                "event_timestamp": (
                    event_time.isoformat().replace("+00:00", "Z")
                ),
                "meter_id": meter["meter_id"],
                "transformer_id": meter["transformer_id"],
                "voltage_v": round(rng.gauss(220.0, 2.0), 2),
                "active_power_kw": round(active_power_kw, 4),
                "reactive_power_kvar": round(reactive_power_kvar, 4),
                "energy_kwh": round(energy_kwh, 4),
                "power_factor": round(power_factor, 4),
                "schema_version": 1,
            })

    return readings


def write_dimensions(output_dir, tables):
    """Write the relatively small dimension tables."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for table_name, rows in tables.items():
        df = pd.DataFrame(rows)

        if table_name == "dim_meter":
            df["tariff_category"] = df["tariff_category"].astype("string")

        path = output_dir / f"{table_name}.parquet"
        df.to_parquet(path, index=False, compression="snappy")
        print(f"Wrote {len(df):,} rows -> {path}")


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

    # All readings are written in bounded batches, not one giant list.
    readings_dir = output_dir / "readings"
    readings_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(config.seed)
    total_readings = 0
    batch_number = 0

    for offset in range(0, len(meters), args.batch_meters):
        meter_batch = meters[offset:offset + args.batch_meters]

        rows = generate_reading_batch(config, meter_batch, rng)
        df = pd.DataFrame(rows)
        df["schema_version"] = df["schema_version"].astype("int32")

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
        del rows, df

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
    print(f"Output directory: {output_dir.resolve()}")

    if total_readings != expected_readings:
        raise RuntimeError(
            f"Reading count mismatch: expected {expected_readings}, "
            f"generated {total_readings}"
        )


if __name__ == "__main__":
    main()

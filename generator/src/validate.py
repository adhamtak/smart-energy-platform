
import json
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data") / "generated-test"

def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    manifest_path = DATA_DIR / "manifest.json"
    require(manifest_path.exists(), "manifest.json is missing.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    require(
        manifest.get("data_type") == "synthetic",
        "Dataset must be explicitly marked synthetic.",
    )

    regions = pd.read_parquet(DATA_DIR / "dim_region.parquet")
    substations = pd.read_parquet(DATA_DIR / "dim_substation.parquet")
    transformers = pd.read_parquet(DATA_DIR / "dim_transformer.parquet")
    meters = pd.read_parquet(DATA_DIR / "dim_meter.parquet")
    readings = pd.read_parquet(DATA_DIR / "readings.parquet")

    # 1. Row counts and manifest consistency
    tables = {
        "dim_region": regions,
        "dim_substation": substations,
        "dim_transformer": transformers,
        "dim_meter": meters,
        "readings": readings,
    }

    print("=== Row counts ===")
    for name, df in tables.items():
        expected = manifest["records"][name]
        print(f"{name}: {len(df):,} rows")
        require(
            len(df) == expected,
            f"{name}: manifest says {expected:,}, found {len(df):,}.",
        )

    expected_readings = (
        manifest["num_meters"] * manifest["num_days"] * 96
    )
    require(
        len(readings) == expected_readings,
        f"Expected {expected_readings:,} readings, "
        f"found {len(readings):,}.",
    )

    # 2. Primary-key uniqueness
    primary_keys = [
        (regions, "region_id"),
        (substations, "substation_id"),
        (transformers, "transformer_id"),
        (meters, "meter_id"),
        (readings, "reading_id"),
    ]

    for df, column in primary_keys:
        require(df[column].notna().all(), f"{column} contains nulls.")
        require(
            not df[column].duplicated().any(),
            f"{column} contains duplicate values.",
        )

    # 3. Foreign-key integrity
    require(
        substations["region_id"].isin(regions["region_id"]).all(),
        "Some substations reference unknown regions.",
    )
    require(
        transformers["region_id"].isin(regions["region_id"]).all(),
        "Some transformers reference unknown regions.",
    )
    require(
        transformers["substation_id"]
        .isin(substations["substation_id"]).all(),
        "Some transformers reference unknown substations.",
    )
    require(
        meters["transformer_id"]
        .isin(transformers["transformer_id"]).all(),
        "Some meters reference unknown transformers.",
    )

    transformer_by_meter = meters.set_index("meter_id")["transformer_id"]
    expected_transformer = readings["meter_id"].map(transformer_by_meter)

    require(
        expected_transformer.notna().all(),
        "Some readings reference unknown meters.",
    )
    require(
        (expected_transformer == readings["transformer_id"]).all(),
        "A reading's transformer_id doesn't match its meter assignment.",
    )

    # 4. Timestamp validity and 15-minute alignment
    timestamps = pd.to_datetime(
        readings["event_timestamp"], utc=True, errors="coerce"
    )
    require(timestamps.notna().all(), "Invalid event timestamps found.")

    aligned = (
        (timestamps.dt.minute % 15 == 0)
        & (timestamps.dt.second == 0)
        & (timestamps.dt.microsecond == 0)
    )
    require(aligned.all(), "Some timestamps aren't 15-minute aligned.")

    # 5. Measurement validity
    for column in [
        "voltage_v",
        "active_power_kw",
        "reactive_power_kvar",
        "energy_kwh",
    ]:
        require(
            pd.to_numeric(readings[column], errors="coerce").notna().all(),
            f"{column} contains missing or nonnumeric values.",
        )

    require((readings["voltage_v"] > 0).all(), "Nonpositive voltage found.")
    require(
        (readings["active_power_kw"] >= 0).all(),
        "Negative active power found.",
    )
    require((readings["energy_kwh"] >= 0).all(), "Negative energy found.")

    # 6. Energy consistency: average kW × 0.25 hours
    expected_energy = readings["active_power_kw"] * 0.25
    energy_error = (readings["energy_kwh"] - expected_energy).abs()

    require(
        (energy_error <= 0.00011).all(),
        "Energy values are inconsistent with active power.",
    )

    # 7. Deterministic reading IDs
    expected_ids = (
        "READ_"
        + readings["meter_id"].astype(str)
        + "_"
        + timestamps.dt.strftime("%Y%m%d%H%M")
    )
    require(
        (expected_ids == readings["reading_id"]).all(),
        "Some reading IDs don't match the meter and UTC interval.",
    )

    # 8. Expected intervals per meter
    counts = readings.groupby("meter_id").size()
    expected_per_meter = manifest["num_days"] * 96
    require(
        (counts == expected_per_meter).all(),
        "Some meters don't have the expected number of readings.",
    )

    print("\nPASS: All initial data-quality checks succeeded.")
    print(f"Synthetic readings validated: {len(readings):,}")
    print(f"Unique meters: {meters['meter_id'].nunique():,}")
    print(f"Unique transformers: {transformers['transformer_id'].nunique():,}")


if __name__ == "__main__":
    main()

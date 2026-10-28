"""Connects to Snowflake, shows what the portal's user can see, and checks the portal's mapping.

    python backend/check_snowflake.py                 # connection + every table/view and its columns
    python backend/check_snowflake.py --meter MTR000001   # also runs the portal's real queries for that meter
    python backend/check_snowflake.py --sample SCHEMA.OBJECT_NAME   # print 5 rows of one table/view

Everything is also saved to backend/snowflake_report.txt (no passwords in it), so you can share it.
Read-only: it only runs SELECT / SHOW / INFORMATION_SCHEMA queries.
"""
import argparse
import re
import sys
from pathlib import Path

import pandas as pd

import snowflake_source as sf

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

REPORT = Path(__file__).resolve().parent / "snowflake_report.txt"
lines = []


def out(text=""):
    print(text)
    lines.append(str(text))


def ident(name):
    return name if re.fullmatch(r"[A-Za-z0-9_$]+", name) else '"' + name.replace('"', '""') + '"'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--meter", help="run the portal's queries for this meter id")
    parser.add_argument("--sample", help="SCHEMA.OBJECT: print 5 rows")
    parser.add_argument("--max-columns", type=int, default=60)
    args = parser.parse_args()
    pd.set_option("display.width", 200, "display.max_columns", 30, "display.max_colwidth", 40)

    out("== 1. Connection ==")
    try:
        who = sf.run("SELECT CURRENT_USER() AS user_name, CURRENT_ROLE() AS role_name, CURRENT_WAREHOUSE() AS warehouse_name, "
                     "CURRENT_DATABASE() AS database_name, CURRENT_SCHEMA() AS schema_name")
    except Exception as error:  # noqa: BLE001
        out(f"FAILED to connect: {error}")
        out("Check SNOWFLAKE_ACCOUNT (format orgname-accountname), user, password/key, role and warehouse in backend/.env")
        REPORT.write_text("\n".join(lines), encoding="utf-8")
        sys.exit(1)
    out(who.T.to_string(header=False))
    database = ident(who["database_name"].iloc[0] or "")

    out("\n== 2. Tables and views this role can see ==")
    objects = sf.run(
        f"SELECT table_schema, table_name, table_type, row_count FROM {database}.INFORMATION_SCHEMA.TABLES "
        "WHERE table_schema <> 'INFORMATION_SCHEMA' ORDER BY table_schema, table_type, table_name"
    )
    if objects.empty:
        out("Nothing visible. Ask the Snowflake owner to GRANT USAGE on the schema and SELECT on the tables/views to this role.")
    else:
        out(objects.to_string(index=False))
        out("(row_count is empty for views)")

    out("\n== 3. Columns ==")
    cols = sf.run(
        f"SELECT table_schema, table_name, column_name, data_type FROM {database}.INFORMATION_SCHEMA.COLUMNS "
        "WHERE table_schema <> 'INFORMATION_SCHEMA' ORDER BY table_schema, table_name, ordinal_position"
    )
    for (schema, name), group in cols.groupby(["table_schema", "table_name"], sort=False):
        shown = [f"{r.column_name} {r.data_type}" for r in group.head(args.max_columns).itertuples()]
        more = f" ... (+{len(group) - args.max_columns} more)" if len(group) > args.max_columns else ""
        out(f"\n{schema}.{name}\n   " + ", ".join(shown) + more)

    out("\n== 4. Does the portal's mapping work? (backend/warehouse_mapping.json or the defaults) ==")
    mapping = sf.load_mapping()
    for logical, spec in mapping.items():
        if not spec["table"]:
            out(f"SKIP  {logical:<10} (no table configured, the portal shows none)")
            continue
        select = sf._select(spec, tuple(spec["columns"]))
        try:
            count = sf.run(f"SELECT COUNT(*) AS n FROM {spec['table']}")["n"].iloc[0]
            sf.run(f"SELECT {select} FROM {spec['table']} LIMIT 1")
            out(f"OK    {logical:<10} <- {spec['table']}  ({count:,} rows)")
        except Exception as error:  # noqa: BLE001
            out(f"FAIL  {logical:<10} <- {spec['table']}\n      {str(error).splitlines()[0]}")
            out("      Fix the table name / column names in backend/warehouse_mapping.json")

    if args.meter:
        out(f"\n== 5. The portal's own queries for {args.meter} ==")
        try:
            readings = sf.readings_for_meter(args.meter)
            out(f"readings: {len(readings)} rows" + (f", {readings['ts'].min()} .. {readings['ts'].max()}" if len(readings) else ""))
            if len(readings):
                out(readings.head(3).to_string(index=False))
                per_day = readings.groupby(readings["ts"].dt.date).size()
                out(f"readings per day: {dict(per_day.head(5))}  (96 per day = one every 15 minutes)")
        except Exception as error:  # noqa: BLE001
            out(f"readings FAILED: {str(error).splitlines()[0]}")
        out(f"meter info: {sf.meter_info(args.meter)}")
        out(f"incidents: {len(sf.incidents_for([args.meter]))} rows")

    if args.sample:
        out(f"\n== Sample of {args.sample} ==")
        try:
            out(sf.run(f"SELECT * FROM {args.sample} LIMIT 5").to_string(index=False))
        except Exception as error:  # noqa: BLE001
            out(f"FAILED: {str(error).splitlines()[0]}")

    REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nSaved to {REPORT}")


if __name__ == "__main__":
    main()

"""Create demo users from meters that really have readings (no change to the data or the contract).

    python backend/make_demo_users.py                  # 10 customers + admin
    python backend/make_demo_users.py --count 25
    python backend/make_demo_users.py --meters MTR000001,MTR000042

Writes backend/users.json (hashed passwords) and backend/demo_credentials.txt (plain passwords,
for you to hand out in the demo). Both are git-ignored.
"""
import argparse
import json
import secrets

from common import (
    PORTAL_DIR, USERS_FILE, METER_RE, discover_layout, duckdb_connection, hash_password,
    readings_source,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10, help="how many customer users")
    parser.add_argument("--meters", help="comma-separated meter ids instead of the first N")
    args = parser.parse_args()

    layout = discover_layout()
    print("Readings read from:")
    for glob in layout["readings"]:
        print("  ", glob)
    print("dim_meter:", layout["dim_meter"] or "not found (customer type will be 'unknown')")
    print("Looking for meters that really have readings (scans the files once, may take a minute)...")

    wanted = None
    if args.meters:
        wanted = [m.strip().upper() for m in args.meters.split(",") if m.strip()]
        bad = [m for m in wanted if not METER_RE.match(m)]
        if bad:
            raise SystemExit(f"Bad meter id(s): {bad}")

    where, params = "", []
    if wanted:
        where = f"WHERE meter_id IN ({','.join('?' * len(wanted))})"
        params = list(wanted)
    limit = len(wanted) if wanted else args.count
    distinct_meters = f"SELECT DISTINCT meter_id FROM {readings_source(layout['readings'])} {where}"

    con = duckdb_connection()
    rows = None
    if layout["dim_meter"]:
        try:  # meters that have readings, with their customer type from dim_meter
            rows = con.execute(
                f"WITH r AS ({distinct_meters}) "
                "SELECT r.meter_id, COALESCE(d.customer_type, 'unknown') "
                f"FROM r LEFT JOIN read_parquet('{layout['dim_meter']}') d ON d.meter_id = r.meter_id "
                "ORDER BY r.meter_id LIMIT ?",
                params + [limit],
            ).fetchall()
        except Exception as error:
            print(f"Could not join dim_meter ({str(error).splitlines()[0]}); continuing without it.")
    if rows is None:
        rows = [
            (r[0], "unknown")
            for r in con.execute(f"{distinct_meters} ORDER BY meter_id LIMIT ?", params + [limit]).fetchall()
        ]

    if not rows:
        raise SystemExit("No meters found in dim_meter.")

    users, lines = {}, []
    for i, (meter_id, customer_type) in enumerate(rows, 1):
        username, password = f"customer{i:03d}", secrets.token_urlsafe(6)
        users[username] = {"password": hash_password(password), "meters": [meter_id]}
        lines.append(f"{username:<12} {password:<12} {meter_id}  ({customer_type})")

    admin_password = secrets.token_urlsafe(8)
    users["admin"] = {"password": hash_password(admin_password), "meters": ["*"]}
    lines.append(f"{'admin':<12} {admin_password:<12} * (all meters)")

    USERS_FILE.write_text(json.dumps(users, indent=2), encoding="utf-8")
    text = f"{'username':<12} {'password':<12} meter\n" + "\n".join(lines)
    (PORTAL_DIR / "demo_credentials.txt").write_text(text + "\n", encoding="utf-8")
    print("\n" + text)
    print(f"\nSaved {USERS_FILE.name} and demo_credentials.txt in {PORTAL_DIR}")


if __name__ == "__main__":
    main()

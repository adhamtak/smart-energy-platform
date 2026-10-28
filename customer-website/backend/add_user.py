"""Add (or update) ONE portal user and link it to one or more meters.

    python backend/add_user.py mariam MTR000001
    python backend/add_user.py ali MTR000007,MTR000008     # a user with two meters
    python backend/add_user.py boss "*"                    # admin: sees every meter

It asks for the password (nothing is shown while you type). Other users in
backend/users.json are kept as they are. Passwords are stored hashed.
"""
import argparse
import getpass
import json
import re

from common import METER_RE, USERS_FILE, hash_password, load_users

USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{3,32}$")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("username")
    parser.add_argument("meters", help='comma-separated meter ids, or "*" for an admin')
    parser.add_argument("--password", help="only for tests; leave it out to be asked safely")
    args = parser.parse_args()

    if not USERNAME_RE.match(args.username):
        raise SystemExit("Username: 3 to 32 characters, letters, digits and _ . - only.")

    meters = [m.strip().upper() for m in args.meters.split(",") if m.strip()]
    if not meters:
        raise SystemExit("Give at least one meter id.")
    if meters != ["*"]:
        bad = [m for m in meters if not METER_RE.match(m)]
        if bad:
            raise SystemExit(f"Bad meter id(s): {bad} (they look like MTR000001)")

    password = args.password
    if not password:
        password = getpass.getpass("Password: ")
        if password != getpass.getpass("Repeat password: "):
            raise SystemExit("The two passwords are different.")
    if len(password) < 8:
        raise SystemExit("Use a password with at least 8 characters.")

    users = load_users()
    existed = args.username in users
    users[args.username] = {"password": hash_password(password), "meters": meters}
    USERS_FILE.write_text(json.dumps(users, indent=2), encoding="utf-8")
    print(f"{'Updated' if existed else 'Created'} user '{args.username}' -> {', '.join(meters)}")


if __name__ == "__main__":
    main()

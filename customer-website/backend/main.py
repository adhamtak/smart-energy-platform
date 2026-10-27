"""FastAPI backend for the Smart Energy customer portal.

Run from the project root:
    uvicorn main:app --reload --host 127.0.0.1 --port 8000 --app-dir backend
Docs (try the endpoints): http://127.0.0.1:8000/docs
"""
import os
import secrets
import time
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

import service
from common import METER_RE, PORTAL_DIR, can_access, hash_password, load_users, verify_password

COOKIE = "portal_session"
SESSION_HOURS = 8


def _load_secret():
    """JWT signing key: PORTAL_JWT_SECRET from .env, else a key saved in backend/.jwt_secret
    (so you are not logged out every time uvicorn reloads)."""
    from_env = os.getenv("PORTAL_JWT_SECRET")
    if from_env:
        return from_env
    key_file = PORTAL_DIR / ".jwt_secret"
    if key_file.is_file():
        return key_file.read_text().strip()
    key = secrets.token_hex(32)
    key_file.write_text(key)
    return key


SECRET = _load_secret()
_DUMMY_HASH = hash_password("not-a-real-password")  # keeps timing similar for unknown usernames

app = FastAPI(title="Smart Energy Portal API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class LoginBody(BaseModel):
    username: str
    password: str


def _me(username, record):
    return {"username": username, "meters": record["meters"], "is_admin": "*" in record["meters"]}


def current_user(request: Request):
    token = request.cookies.get(COOKIE)
    if not token:
        raise HTTPException(401, "Not signed in")
    try:
        claims = jwt.decode(token, SECRET, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(401, "Session expired, please sign in again")
    record = load_users().get(claims.get("sub"))
    if not record:
        raise HTTPException(401, "Unknown user")
    return {"username": claims["sub"], **record}


def authorize(user, meter_id: str):
    meter_id = meter_id.strip().upper()
    if not METER_RE.match(meter_id):
        raise HTTPException(400, "Meter IDs look like MTR000001")
    if not can_access(user, meter_id):  # you may only see your own meters
        raise HTTPException(403, "You do not have access to this meter")
    return meter_id


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/auth/login")
def login(body: LoginBody, response: Response):
    users = load_users()
    if not users:
        raise HTTPException(500, "No users yet. Run: python backend/make_demo_users.py")
    username = body.username.strip()
    record = users.get(username)
    ok = verify_password(body.password, record["password"] if record else _DUMMY_HASH)
    if not (record and ok):
        time.sleep(1)  # slow down password guessing
        raise HTTPException(401, "Wrong username or password")

    token = jwt.encode(
        {"sub": username, "exp": datetime.now(timezone.utc) + timedelta(hours=SESSION_HOURS)},
        SECRET,
        algorithm="HS256",
    )
    response.set_cookie(
        COOKIE, token, httponly=True, samesite="lax", max_age=SESSION_HOURS * 3600, path="/"
    )  # add secure=True when you serve over HTTPS
    return _me(username, record)


@app.post("/api/auth/logout")
def logout(response: Response):
    response.delete_cookie(COOKIE, path="/")
    return {"ok": True}


@app.get("/api/me")
def me(user=Depends(current_user)):
    return _me(user["username"], user)


@app.get("/api/meters/{meter_id}")
def meter(meter_id: str, user=Depends(current_user)):
    meter_id = authorize(user, meter_id)
    try:
        return service.build_payload(meter_id)
    except service.NoData as error:
        raise HTTPException(404, str(error))
    except Exception as error:
        raise HTTPException(503, f"Could not read the data lake: {error}")


@app.get("/api/meters/{meter_id}/readings.csv")
def meter_csv(meter_id: str, user=Depends(current_user)):
    meter_id = authorize(user, meter_id)
    try:
        csv_text = service.readings_csv(meter_id)
    except service.NoData as error:
        raise HTTPException(404, str(error))
    except Exception as error:
        raise HTTPException(503, f"Could not read the data lake: {error}")
    return PlainTextResponse(
        csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{meter_id}_readings.csv"'},
    )


@app.post("/api/admin/refresh")
def refresh(user=Depends(current_user)):
    if "*" not in user["meters"]:
        raise HTTPException(403, "Admins only")
    service.clear_cache()
    return {"ok": True}

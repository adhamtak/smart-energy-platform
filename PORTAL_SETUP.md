# Customer portal: FastAPI + React

```
backend/   FastAPI API (login, per-meter authorization, reads Parquet from MinIO with DuckDB)
frontend/  React + Vite UI (login, meter page, charts, incidents, CSV download)
```

## Run (two terminals, from the project root)

Terminal 1, backend (venv active):
```powershell
pip install -r backend/requirements.txt
python backend/where_is_my_data.py                 # optional: shows what is in MinIO
python backend/make_demo_users.py --count 10       # prints usernames + passwords
uvicorn main:app --reload --host 127.0.0.1 --port 8000 --app-dir backend
```
API docs: http://127.0.0.1:8000/docs

Terminal 2, frontend:
```powershell
cd frontend
npm install
npm run dev
```
Open http://localhost:5173 and sign in (`admin` sees any meter, `customer001` only its own).

## API

| Method | Path | Notes |
|---|---|---|
| POST | /api/auth/login | sets an httpOnly session cookie (8 h) |
| POST | /api/auth/logout | |
| GET | /api/me | current user + allowed meters |
| GET | /api/meters/{id} | summary, hourly/daily/15-min series, incidents (403 if not your meter) |
| GET | /api/meters/{id}/readings.csv | CSV download |
| POST | /api/admin/refresh | admin only, clears the 5-min cache |

## Notes
- Needs `docker compose up -d` (MinIO) and the readings uploaded.
- DuckDB downloads its `httpfs` extension on first use (internet needed once).
- Demo-grade security. For production: HTTPS + `secure=True` cookie, a real user store, rate limiting.
- All data is synthetic.

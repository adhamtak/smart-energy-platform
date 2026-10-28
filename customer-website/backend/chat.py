"""Chat assistant for the portal.

How it works (no paid service needed):
  1. The model (a local Ollama model) never sees the database. It can only CALL the small, fixed
     "tools" below. Every tool runs a safe, read-only calculation on the same data the dashboard uses.
  2. The numbers in the answer come from the tool results, so the bot cannot make them up.
  3. Access rules are the same as the dashboard: a customer can only ask about their own meters,
     transformer-level questions are for admin (engineer) accounts.
  4. If Ollama is not running (or the model answers without using a tool but quotes numbers), a simple
     keyword router answers the common questions directly, so the demo never breaks.

Settings (backend/.env, all optional):
    OLLAMA_URL=http://127.0.0.1:11434
    OLLAMA_MODEL=qwen2.5:7b
    OLLAMA_TIMEOUT_S=180
"""
import inspect
import json
import os
import re
import urllib.error
import urllib.request

import pandas as pd

import service
import snowflake_source as sf
from common import METER_RE, VOLTAGE_LOW_V, can_access, summarize

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT_S", "180"))
MAX_TOOL_ROUNDS = 4
MAX_TRANSFORMER_METERS = int(os.getenv("CHAT_MAX_TRANSFORMER_METERS", "30"))
READINGS_PER_DAY = 96  # 15-minute readings


# ------------------------------------------------------------------ errors (English + Arabic)
ERRORS = {
    "bad_meter": ("Give a meter id like MTR000001.", "اكتب رقم عدّاد مثل MTR000001."),
    "no_access": ("This account has no access to {id}.", "هذا الحساب لا يملك صلاحية عرض {id}."),
    "no_meter": ("There is no meter named {id} in the data.", "لا يوجد عدّاد باسم {id} في البيانات."),
    "bad_transformer": ("Give a transformer id like TX_NCAI_0001.", "اكتب رقم محوّل مثل TX_NCAI_0001."),
    "admin_only": (
        "Transformer-level questions are available to engineer (admin) accounts only.",
        "أسئلة المحوّلات متاحة لحسابات المهندسين (المشرفين) فقط.",
    ),
    "no_transformer_meters": ("No meters found on transformer {id}.", "لا توجد عدّادات على المحوّل {id}."),
    "no_transformer_data": ("No readings found for the meters of transformer {id}.", "لا توجد قراءات لعدّادات المحوّل {id}."),
}


class ToolError(Exception):
    def __init__(self, code, **values):
        self.code, self.values = code, values
        super().__init__(ERRORS[code][0].format(**values))

    def text(self, lang):
        return ERRORS[self.code][1 if lang == "ar" else 0].format(**self.values)


# ------------------------------------------------------------------ helpers
def _n(value, default, lo=1, hi=10):
    try:
        return max(lo, min(hi, int(value)))
    except (TypeError, ValueError):
        return default


def _when(ts):
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M")


def _hour(h):
    return f"{int(h):02d}:00"


def _is_admin(user):
    return "*" in user.get("meters", [])


def _meter(user, meter_id):
    mid = str(meter_id or "").strip().upper()
    if not METER_RE.match(mid):
        raise ToolError("bad_meter")
    if not can_access(user, mid):
        raise ToolError("no_access", id=mid)
    return mid


def _frame(user, meter_id):
    mid = _meter(user, meter_id)
    try:
        return mid, service.get_frame(mid)
    except service.NoData:
        raise ToolError("no_meter", id=mid)


def _info(mid):
    layout = service.get_layout()
    return service.cached(("info", layout["dim_meter"], mid), 600, lambda: service.fetch_meter_info(layout, mid))


def _incident_rows(rows, limit=10):
    keep = ("detected_at", "severity", "anomaly_type", "asset_id", "status", "observed_value", "threshold")
    return [{k: r.get(k) for k in keep} for r in rows[:limit]]


# ------------------------------------------------------------------ meter tools
def meter_summary(user, meter_id):
    mid, df = _frame(user, meter_id)
    s = summarize(df)
    info = _info(mid)
    return {
        "meter_id": mid,
        "customer_type": info.get("customer_type"),
        "transformer_id": info.get("transformer_id"),
        "period_from": _when(df["ts_local"].min()),
        "period_to": _when(df["ts_local"].max()),
        "n_readings": s["n_readings"],
        "total_kwh": round(s["total_kwh"], 2),
        "avg_daily_kwh": round(s["avg_daily_kwh"], 2),
        "peak_kw": round(s["peak_kw"], 3),
        "peak_time": _when(s["peak_time"]),
        "min_voltage_v": round(s["min_voltage"], 1),
        "avg_voltage_v": round(s["avg_voltage"], 1),
        "avg_power_factor": round(s["avg_power_factor"], 3),
        "low_voltage_limit_v": VOLTAGE_LOW_V,
        "readings_below_limit": s["low_voltage_intervals"],
    }


def voltage_extremes(user, meter_id, which="highest", n=5):
    mid, df = _frame(user, meter_id)
    which = "lowest" if str(which).lower().startswith("low") else "highest"
    n = _n(n, 5)
    rows = df.nsmallest(n, "voltage_v") if which == "lowest" else df.nlargest(n, "voltage_v")
    return {
        "meter_id": mid,
        "which": which,
        "readings": [{"time": _when(r.ts_local), "voltage_v": round(float(r.voltage_v), 1)} for r in rows.itertuples()],
        "low_voltage_limit_v": VOLTAGE_LOW_V,
        "note": "No official high-voltage limit exists in the data; these are just the highest recorded readings.",
    }


def voltage_by_hour(user, meter_id):
    mid, df = _frame(user, meter_id)
    avg = df.groupby(df["ts_local"].dt.hour)["voltage_v"].mean().round(1)
    below = df[df["voltage_v"] < VOLTAGE_LOW_V]
    low_by_hour = below.groupby(below["ts_local"].dt.hour).size().sort_values(ascending=False).head(3)
    return {
        "meter_id": mid,
        "hours_with_highest_avg_voltage": [{"hour": _hour(h), "avg_voltage_v": float(v)} for h, v in avg.sort_values(ascending=False).head(3).items()],
        "hours_with_lowest_avg_voltage": [{"hour": _hour(h), "avg_voltage_v": float(v)} for h, v in avg.sort_values().head(3).items()],
        "low_voltage_limit_v": VOLTAGE_LOW_V,
        "readings_below_limit": int(len(below)),
        "hours_with_most_readings_below_limit": [{"hour": _hour(h), "readings": int(c)} for h, c in low_by_hour.items()],
    }


def peak_load(user, meter_id, n=3):
    mid, df = _frame(user, meter_id)
    rows = df.nlargest(_n(n, 3), "active_power_kw")
    return {"meter_id": mid, "peaks": [{"time": _when(r.ts_local), "load_kw": round(float(r.active_power_kw), 3)} for r in rows.itertuples()]}


def usage_by_hour(user, meter_id, n=3):
    mid, df = _frame(user, meter_id)
    hourly = df.groupby(df["ts_local"].dt.floor("h"))["energy_kwh"].sum()
    by_hod = hourly.groupby(hourly.index.hour).mean().round(3)
    n = _n(n, 3)
    return {
        "meter_id": mid,
        "busiest_hours": [{"hour": _hour(h), "avg_kwh": float(v)} for h, v in by_hod.sort_values(ascending=False).head(n).items()],
        "quietest_hours": [{"hour": _hour(h), "avg_kwh": float(v)} for h, v in by_hod.sort_values().head(2).items()],
    }


def daily_usage(user, meter_id):
    mid, df = _frame(user, meter_id)
    by_day = df.groupby("date").agg(kwh=("energy_kwh", "sum"), readings=("energy_kwh", "size")).reset_index().sort_values("date")
    by_day["complete"] = by_day["readings"] >= READINGS_PER_DAY
    pool = by_day[by_day["complete"]] if by_day["complete"].any() else by_day
    best = pool.loc[pool["kwh"].idxmax()]
    days = [{"date": str(r.date), "kwh": round(float(r.kwh), 2), "complete_day": bool(r.complete)} for r in by_day.tail(14).itertuples()]
    return {
        "meter_id": mid,
        "highest_day": {"date": str(best["date"]), "kwh": round(float(best["kwh"]), 2), "complete_day": bool(best["complete"])},
        "days": days,
        "note": "Days with fewer than 96 readings are partial (the period starts or ends mid-day).",
    }


def meter_incidents(user, meter_id):
    mid, _ = _frame(user, meter_id)
    info = _info(mid)
    assets = [mid] + ([str(info["transformer_id"])] if info.get("transformer_id") else [])
    rows = service.cached(("incidents", tuple(assets)), 60, lambda: service.fetch_incidents(assets))
    return {"meter_id": mid, "checked_assets": assets, "count": len(rows), "incidents": _incident_rows(rows)}


# ------------------------------------------------------------------ transformer tools (admin only)
def _require_admin(user):
    if not _is_admin(user):
        raise ToolError("admin_only")


def _tid(value):
    tid = str(value or "").strip().upper()
    if not re.match(r"^[A-Z0-9_\-]{3,40}$", tid):
        raise ToolError("bad_transformer")
    return tid


def meters_of_transformer(tid):
    if service.USE_SNOWFLAKE:
        spec = sf.load_mapping()["dim_meter"]
        c = spec["columns"]
        frame = sf.run(f"SELECT {c['meter_id']} AS meter_id FROM {spec['table']} WHERE {c['transformer_id']} = ? ORDER BY 1", [tid])
    else:
        layout = service.get_layout()
        if not layout["dim_meter"]:
            return []
        frame = service._query(f"SELECT meter_id FROM read_parquet('{layout['dim_meter']}') WHERE transformer_id = ? ORDER BY meter_id", [tid])
    return [str(m) for m in frame["meter_id"]]


def _transformer_data(user, transformer_id):
    _require_admin(user)
    tid = _tid(transformer_id)
    ids = meters_of_transformer(tid)
    if not ids:
        raise ToolError("no_transformer_meters", id=tid)
    frames = []
    for mid in ids[:MAX_TRANSFORMER_METERS]:
        try:
            frames.append(service.get_frame(mid)[["ts", "ts_local", "voltage_v", "active_power_kw", "energy_kwh"]].assign(meter_id=mid))
        except service.NoData:
            continue
    if not frames:
        raise ToolError("no_transformer_data", id=tid)
    return tid, ids, pd.concat(frames, ignore_index=True)


def transformer_overview(user, transformer_id):
    tid, ids, df = _transformer_data(user, transformer_id)
    combined = df.groupby("ts")["active_power_kw"].sum()
    peak_ts = combined.idxmax()
    low = df[df["voltage_v"] < VOLTAGE_LOW_V].groupby("meter_id").size().sort_values(ascending=False).head(5)
    lo, hi = df.loc[df["voltage_v"].idxmin()], df.loc[df["voltage_v"].idxmax()]
    return {
        "transformer_id": tid,
        "meters_total": len(ids),
        "meters_analyzed": int(df["meter_id"].nunique()),
        "analysis_capped": len(ids) > MAX_TRANSFORMER_METERS,
        "total_kwh": round(float(df["energy_kwh"].sum()), 2),
        "combined_peak_kw": round(float(combined.max()), 3),
        "combined_peak_time": _when(df.loc[df["ts"] == peak_ts, "ts_local"].iloc[0]),
        "lowest_voltage": {"meter_id": lo["meter_id"], "time": _when(lo["ts_local"]), "voltage_v": round(float(lo["voltage_v"]), 1)},
        "highest_voltage": {"meter_id": hi["meter_id"], "time": _when(hi["ts_local"]), "voltage_v": round(float(hi["voltage_v"]), 1)},
        "low_voltage_limit_v": VOLTAGE_LOW_V,
        "meters_with_most_readings_below_limit": [{"meter_id": m, "readings": int(c)} for m, c in low.items()],
    }


def transformer_voltage_extremes(user, transformer_id, which="highest", n=5):
    tid, ids, df = _transformer_data(user, transformer_id)
    which = "lowest" if str(which).lower().startswith("low") else "highest"
    n = _n(n, 5)
    rows = df.nsmallest(n, "voltage_v") if which == "lowest" else df.nlargest(n, "voltage_v")
    return {
        "transformer_id": tid,
        "which": which,
        "meters_analyzed": int(df["meter_id"].nunique()),
        "readings": [{"meter_id": r.meter_id, "time": _when(r.ts_local), "voltage_v": round(float(r.voltage_v), 1)} for r in rows.itertuples()],
        "low_voltage_limit_v": VOLTAGE_LOW_V,
    }


def transformer_incidents(user, transformer_id):
    _require_admin(user)
    tid = _tid(transformer_id)
    rows = service.cached(("incidents", (tid,)), 60, lambda: service.fetch_incidents([tid]))
    return {"transformer_id": tid, "count": len(rows), "incidents": _incident_rows(rows)}


# ------------------------------------------------------------------ tool registry (what the model sees)
def _spec(name, description, props=None, required=()):
    return {"type": "function", "function": {"name": name, "description": description, "parameters": {"type": "object", "properties": props or {}, "required": list(required)}}}


_METER = {"meter_id": {"type": "string", "description": "Meter id like MTR000001. Leave out to use the meter currently on screen."}}
_TRANSFORMER = {"transformer_id": {"type": "string", "description": "Transformer id like TX_NCAI_0001."}}
_WHICH = {"which": {"type": "string", "enum": ["highest", "lowest"]}, "n": {"type": "integer", "description": "How many readings (1-10)."}}

TOOLS = {
    "meter_summary": (meter_summary, False, _spec("meter_summary", "Overall numbers for a meter: total and daily consumption, peak load, voltage min/avg, power factor, readings below the low-voltage limit.", _METER)),
    "voltage_extremes": (voltage_extremes, False, _spec("voltage_extremes", "The highest or lowest voltage readings of a meter with their exact times.", {**_METER, **_WHICH})),
    "voltage_by_hour": (voltage_by_hour, False, _spec("voltage_by_hour", "Which hours of the day the voltage is usually highest/lowest, and when low-voltage readings happen most.", _METER)),
    "peak_load": (peak_load, False, _spec("peak_load", "The highest power draws (kW) of a meter and when they happened.", {**_METER, "n": {"type": "integer"}})),
    "usage_by_hour": (usage_by_hour, False, _spec("usage_by_hour", "Which hours of the day a meter uses the most / least electricity on average.", {**_METER, "n": {"type": "integer"}})),
    "daily_usage": (daily_usage, False, _spec("daily_usage", "Energy used per day and the day with the highest consumption.", _METER)),
    "meter_incidents": (meter_incidents, False, _spec("meter_incidents", "Incidents (anomalies) recorded for a meter or its transformer.", _METER)),
    "transformer_overview": (transformer_overview, True, _spec("transformer_overview", "Summary of a transformer across its meters: consumption, combined peak load, lowest/highest voltage, meters with most low-voltage readings.", _TRANSFORMER, ["transformer_id"])),
    "transformer_voltage_extremes": (transformer_voltage_extremes, True, _spec("transformer_voltage_extremes", "Highest or lowest voltage readings across all meters of a transformer, with meter and time.", {**_TRANSFORMER, **_WHICH}, ["transformer_id"])),
    "transformer_incidents": (transformer_incidents, True, _spec("transformer_incidents", "Incidents recorded for a transformer.", _TRANSFORMER, ["transformer_id"])),
}


def tool_specs(user):
    return [spec for _, admin_only, spec in TOOLS.values() if _is_admin(user) or not admin_only]


def run_tool(user, name, args, ctx_meter=None):
    """Run one tool safely. Always returns a JSON-able dict ({"error": ...} on problems)."""
    entry = TOOLS.get(name)
    if entry is None:
        return {"error": f"Unknown tool '{name}'."}
    func, _, _ = entry
    args = dict(args or {})
    params = inspect.signature(func).parameters
    if "meter_id" in params and not args.get("meter_id") and ctx_meter:
        args["meter_id"] = ctx_meter
    args = {k: v for k, v in args.items() if k in params}
    try:
        return func(user, **args)
    except ToolError as error:
        return {"error": str(error), "code": error.code, "values": error.values}
    except Exception as error:  # noqa: BLE001
        return {"error": f"Could not read the data: {error}"}


# ------------------------------------------------------------------ rule-based answers (no AI needed)
def _norm(text):
    text = re.sub(r"[\u064b-\u065f\u0640]", "", str(text).lower())
    return text.translate(str.maketrans({"أ": "ا", "إ": "ا", "آ": "ا", "ى": "ي", "ة": "ه"}))


KEYWORDS = {
    "voltage": ("voltage", "volt", "جهد", "فولت"),
    "high": ("high", "max", "عال", "اعلي", "ارتفع", "اقصي", "اكبر"),
    "low": ("low", "min", "drop", "dip", "واطي", "اقل", "منخفض", "هبوط", "نزل"),
    "most": ("most", "usually", "often", "اكتر", "اكثر", "غالبا"),
    "when": ("when", "time", "hour", "وقت", "امتي", "ساعه", "متي"),
    "peak": ("peak", "load", "ذروه", "حمل"),
    "day": ("day", "daily", "يوم", "ايام"),
    "incident": ("incident", "alert", "تنبيه", "عطل", "اعطال", "مشكل", "حوادث"),
    "transformer": ("transformer", "محول"),
    "usage": ("consum", "usage", "use", "استهلاك", "استهلك", "كهرب", "total", "اجمالي", "summary", "ملخص"),
}


def _has(text, key):
    return any(word in text for word in KEYWORDS[key])


def _route(user, message, ctx_meter):
    """Pick a tool (and arguments) from the wording. Returns (tool_name, args, which) or None."""
    text = _norm(message)
    upper = message.upper()
    meter = (re.search(r"MTR\d{6}", upper) or [None])[0] or ctx_meter
    tid = (re.search(r"TX_[A-Z0-9_]+", upper) or [None])[0]
    which = "lowest" if (_has(text, "low") and not _has(text, "high")) else "highest"

    if _is_admin(user) and (tid or _has(text, "transformer")):
        if not tid:
            return ("transformer_overview", {"transformer_id": ""}, which)
        if _has(text, "incident"):
            return ("transformer_incidents", {"transformer_id": tid}, which)
        if _has(text, "voltage") and (_has(text, "high") or _has(text, "low")):
            return ("transformer_voltage_extremes", {"transformer_id": tid, "which": which}, which)
        return ("transformer_overview", {"transformer_id": tid}, which)

    args = {"meter_id": meter}
    if _has(text, "incident"):
        return ("meter_incidents", args, which)
    if _has(text, "voltage"):
        if _has(text, "most") or (_has(text, "when") and not (_has(text, "high") or _has(text, "low"))):
            return ("voltage_by_hour", args, which)
        if _has(text, "high") or _has(text, "low"):
            return ("voltage_extremes", {**args, "which": which}, which)
        return ("meter_summary", args, which)
    if _has(text, "peak"):
        return ("peak_load", args, which)
    if _has(text, "day") and _has(text, "usage"):
        return ("daily_usage", args, which)
    if _has(text, "when") and _has(text, "usage"):
        return ("usage_by_hour", args, which)
    if _has(text, "usage") or _has(text, "most") or re.search(r"MTR\d{6}", upper):
        return ("meter_summary", args, which)
    return None


def _units(lang):
    return {"kwh": "كيلوواط‑ساعة", "kw": "كيلوواط", "v": "فولت"} if lang == "ar" else {"kwh": "kWh", "kw": "kW", "v": "V"}


def _format(name, r, which, lang):
    ar, U = lang == "ar", _units(lang)
    pick = lambda en, a: a if ar else en  # noqa: E731
    if name == "meter_summary":
        return pick(
            f"{r['meter_id']}: {r['total_kwh']} {U['kwh']} in total (about {r['avg_daily_kwh']} {U['kwh']} a day). Peak load {r['peak_kw']} {U['kw']} on {r['peak_time']}. "
            f"Voltage: lowest {r['min_voltage_v']} {U['v']}, average {r['avg_voltage_v']} {U['v']}; {r['readings_below_limit']} of {r['n_readings']} readings were below {r['low_voltage_limit_v']:g} {U['v']}.",
            f"{r['meter_id']}: إجمالي الاستهلاك {r['total_kwh']} {U['kwh']} (نحو {r['avg_daily_kwh']} {U['kwh']} يوميًا). أعلى حمل {r['peak_kw']} {U['kw']} بتاريخ {r['peak_time']}. "
            f"الجهد: أقل قيمة {r['min_voltage_v']} {U['v']} والمتوسط {r['avg_voltage_v']} {U['v']}، و{r['readings_below_limit']} من {r['n_readings']} قراءة كانت أقل من {r['low_voltage_limit_v']:g} {U['v']}.",
        )
    if name == "voltage_extremes":
        lines = "\n".join(f"• {x['time']}: {x['voltage_v']} {U['v']}" for x in r["readings"])
        head = pick(f"{'Lowest' if r['which'] == 'lowest' else 'Highest'} voltage readings for {r['meter_id']}:", f"{'أقل' if r['which'] == 'lowest' else 'أعلى'} قراءات الجهد للعدّاد {r['meter_id']}:")
        return f"{head}\n{lines}"
    if name == "voltage_by_hour":
        key = "hours_with_lowest_avg_voltage" if which == "lowest" else "hours_with_highest_avg_voltage"
        lines = "\n".join(f"• {x['hour']}: {x['avg_voltage_v']} {U['v']}" for x in r[key])
        head = pick(f"Hours of the day with the {'lowest' if which == 'lowest' else 'highest'} average voltage ({r['meter_id']}):", f"ساعات اليوم التي يكون فيها متوسط الجهد {'الأقل' if which == 'lowest' else 'الأعلى'} ({r['meter_id']}):")
        extra = ""
        if r["readings_below_limit"]:
            hrs = "، ".join(f"{x['hour']} ({x['readings']})" for x in r["hours_with_most_readings_below_limit"]) if ar else ", ".join(f"{x['hour']} ({x['readings']})" for x in r["hours_with_most_readings_below_limit"])
            extra = pick(f"\n{r['readings_below_limit']} readings were below {r['low_voltage_limit_v']:g} {U['v']}, mostly at: {hrs}.", f"\nكانت {r['readings_below_limit']} قراءة أقل من {r['low_voltage_limit_v']:g} {U['v']}، وأغلبها عند: {hrs}.")
        return f"{head}\n{lines}{extra}"
    if name == "peak_load":
        lines = "\n".join(f"• {x['time']}: {x['load_kw']} {U['kw']}" for x in r["peaks"])
        return pick(f"Highest loads for {r['meter_id']}:\n{lines}", f"أعلى حمل للعدّاد {r['meter_id']}:\n{lines}")
    if name == "usage_by_hour":
        top = "\n".join(f"• {x['hour']}: {x['avg_kwh']} {U['kwh']}" for x in r["busiest_hours"])
        quiet = ", ".join(x["hour"] for x in r["quietest_hours"])
        return pick(f"Busiest hours for {r['meter_id']} (average energy per hour):\n{top}\nQuietest: {quiet}.", f"أكثر الساعات استهلاكًا للعدّاد {r['meter_id']} (متوسط الطاقة في الساعة):\n{top}\nأهدأ الساعات: {quiet}.")
    if name == "daily_usage":
        d = r["highest_day"]
        partial = pick(" (a partial day)", " (يوم غير مكتمل)") if not d["complete_day"] else ""
        return pick(f"{r['meter_id']} used the most on {d['date']}: {d['kwh']} {U['kwh']}{partial}.", f"أعلى يوم استهلاكًا للعدّاد {r['meter_id']} كان {d['date']}: {d['kwh']} {U['kwh']}{partial}.")
    if name in ("meter_incidents", "transformer_incidents"):
        who = r.get("meter_id") or r.get("transformer_id")
        if not r["count"]:
            return pick(f"No incidents are recorded for {who}.", f"لا توجد تنبيهات مسجّلة لـ {who}.")
        lines = "\n".join(f"• {x['detected_at']}: {x['anomaly_type']} ({x['severity']}), {x['status']}" for x in r["incidents"])
        return pick(f"{r['count']} incident(s) for {who}:\n{lines}", f"{r['count']} تنبيه لـ {who}:\n{lines}")
    if name == "transformer_overview":
        lo, hi = r["lowest_voltage"], r["highest_voltage"]
        capped = pick(f" (first {r['meters_analyzed']} meters only)", f" (أول {r['meters_analyzed']} عدّاد فقط)") if r["analysis_capped"] else ""
        worst = ", ".join(f"{x['meter_id']} ({x['readings']})" for x in r["meters_with_most_readings_below_limit"])
        tail_en = f"\nMost readings below {r['low_voltage_limit_v']:g} {U['v']}: {worst}." if worst else ""
        tail_ar = f"\nأكثر العدّادات قراءات أقل من {r['low_voltage_limit_v']:g} {U['v']}: {worst}." if worst else ""
        return pick(
            f"{r['transformer_id']} ({r['meters_total']} meters{capped}): {r['total_kwh']} {U['kwh']} in total. Combined peak {r['combined_peak_kw']} {U['kw']} on {r['combined_peak_time']}. "
            f"Lowest voltage {lo['voltage_v']} {U['v']} ({lo['meter_id']}, {lo['time']}); highest {hi['voltage_v']} {U['v']} ({hi['meter_id']}, {hi['time']}).{tail_en}",
            f"{r['transformer_id']} ({r['meters_total']} عدّاد{capped}): إجمالي الاستهلاك {r['total_kwh']} {U['kwh']}. أعلى حمل مجمّع {r['combined_peak_kw']} {U['kw']} بتاريخ {r['combined_peak_time']}. "
            f"أقل جهد {lo['voltage_v']} {U['v']} ({lo['meter_id']}، {lo['time']})، وأعلى جهد {hi['voltage_v']} {U['v']} ({hi['meter_id']}، {hi['time']}).{tail_ar}",
        )
    if name == "transformer_voltage_extremes":
        lines = "\n".join(f"• {x['meter_id']}، {x['time']}: {x['voltage_v']} {U['v']}" if ar else f"• {x['meter_id']}, {x['time']}: {x['voltage_v']} {U['v']}" for x in r["readings"])
        head = pick(f"{'Lowest' if r['which'] == 'lowest' else 'Highest'} voltage readings on {r['transformer_id']}:", f"{'أقل' if r['which'] == 'lowest' else 'أعلى'} قراءات الجهد على {r['transformer_id']}:")
        return f"{head}\n{lines}"
    return json.dumps(r, ensure_ascii=False)


def _error_text(result, lang):
    code = result.get("code")
    if code in ERRORS:
        return ERRORS[code][1 if lang == "ar" else 0].format(**result.get("values", {}))
    return result["error"]


def rules_answer(user, message, ctx_meter, lang):
    """Answer a common question without any AI. Returns text, or None if the question isn't recognised."""
    routed = _route(user, message, ctx_meter)
    if not routed:
        return None, []
    name, args, which = routed
    result = run_tool(user, name, args, ctx_meter)
    if "error" in result:
        return _error_text(result, lang), [name]
    return _format(name, result, which, lang), [name]


HELP = {
    "en": "I can answer from your meter data: voltage (highest/lowest, and at which hours), peak load, usage by hour or by day, totals, and incidents. Try: “When was my voltage highest?”",
    "ar": "أستطيع الإجابة من بيانات عدّادك: الجهد (الأعلى والأقل وفي أي ساعات)، وأعلى حمل، والاستهلاك بالساعة أو باليوم، والإجماليات، والتنبيهات. جرّب: «متى كان الجهد الأعلى؟»",
}
NO_AI_NOTE = {
    "en": "The local AI model is not running, so I can only answer a few fixed questions. ",
    "ar": "نموذج الذكاء الاصطناعي المحلي غير مُشغَّل، لذلك أجيب عن أسئلة محددة فقط. ",
}


# ------------------------------------------------------------------ Ollama
class OllamaUnavailable(Exception):
    pass


def _ollama_chat(messages, tools):
    payload = {"model": OLLAMA_MODEL, "messages": messages, "stream": False, "options": {"temperature": 0.1}}
    if tools:
        payload["tools"] = tools
    request = urllib.request.Request(f"{OLLAMA_URL}/api/chat", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=OLLAMA_TIMEOUT) as response:
            return json.loads(response.read().decode())["message"]
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="ignore")[:200]
        raise OllamaUnavailable(f"Ollama answered {error.code}: {detail}")
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError, KeyError, ValueError) as error:
        raise OllamaUnavailable(str(error))


def _system_prompt(ctx_meter, lang, admin):
    scope = "The user is an engineer (admin) and may ask about any meter or transformer." if admin else "The user is a customer and may only ask about their own meter(s)."
    return (
        "You are the assistant of the Smart Energy customer portal. "
        "Answer ONLY with numbers returned by the tools. For any question about readings, voltage, consumption, load or incidents, call a tool first. "
        "Never invent numbers, dates, meter ids or thresholds. If no tool can answer, say you don't have that data. "
        f"All times are Africa/Cairo local time. The low-voltage limit is {VOLTAGE_LOW_V:g} V. There is no official high-voltage limit in the data: "
        "for 'high voltage' report the highest recorded readings and do not call them dangerous. "
        f"The meter currently on screen is {ctx_meter or 'unknown'}; use it when the user does not name a meter. {scope} "
        f"Reply in the user's language ({'Arabic' if lang == 'ar' else 'English'} unless they write in the other). Keep it short, in plain words, with units (kWh, kW, V) and Western digits."
    )


def _clean_history(history):
    out = []
    for item in (history or [])[-6:]:
        if isinstance(item, dict) and item.get("role") in ("user", "assistant") and isinstance(item.get("content"), str):
            out.append({"role": item["role"], "content": item["content"][:600]})
    return out


def answer(user, message, meter_id=None, lang="en", history=None):
    """Return {"reply": str, "engine": "ollama" | "rules", "tools": [names]}."""
    lang = "ar" if lang == "ar" else "en"
    ctx_meter = str(meter_id or "").strip().upper() or None
    if ctx_meter and not METER_RE.match(ctx_meter):
        ctx_meter = None

    messages = [{"role": "system", "content": _system_prompt(ctx_meter, lang, _is_admin(user))}] + _clean_history(history) + [{"role": "user", "content": message}]
    specs, used = tool_specs(user), []

    try:
        for _ in range(MAX_TOOL_ROUNDS):
            reply = _ollama_chat(messages, specs)
            calls = reply.get("tool_calls") or []
            if not calls:
                text = (reply.get("content") or "").strip()
                if not used and re.search(r"\d", text):
                    # numbers without a tool call = possibly invented, so answer from the data instead
                    fallback, names = rules_answer(user, message, ctx_meter, lang)
                    return {"reply": fallback or HELP[lang], "engine": "rules", "tools": names}
                return {"reply": text or HELP[lang], "engine": "ollama", "tools": used}
            messages.append({"role": "assistant", "content": reply.get("content") or "", "tool_calls": calls})
            for call in calls:
                function = call.get("function", {})
                args = function.get("arguments") or {}
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except ValueError:
                        args = {}
                name = function.get("name", "")
                used.append(name)
                messages.append({"role": "tool", "tool_name": name, "content": json.dumps(run_tool(user, name, args, ctx_meter), ensure_ascii=False, default=str)})
        final = _ollama_chat(messages, None)  # tool budget used up: ask for the answer with what we have
        return {"reply": (final.get("content") or "").strip() or HELP[lang], "engine": "ollama", "tools": used}
    except OllamaUnavailable:
        fallback, names = rules_answer(user, message, ctx_meter, lang)
        if fallback:
            return {"reply": fallback, "engine": "rules", "tools": names}
        return {"reply": NO_AI_NOTE[lang] + HELP[lang], "engine": "rules", "tools": []}

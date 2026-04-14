"""
Ladbrokes One API service for AU Greyhound races.

Endpoint: https://one-api.ladbrokes.com/v4/sportsbook-api
Auth: api-key query parameter
Category 19 = Greyhounds

Used for:
1. Schedule (race list with tracks, times, runners)
2. LADS prices (from selections)
"""
import os
import time
import logging
import requests
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger(__name__)

API_KEY = os.environ.get("LADBROKES_API_KEY", "LD0d4820596d8040c8b242b13771cf2449")
BASE_URL = "https://one-api.ladbrokes.com/v4/sportsbook-api"
CATEGORY = 19

_cache = {"events": None, "timestamp": 0, "ttl": 60}


def _params(extra=None):
    p = {"locale": "en-GB", "api-key": API_KEY}
    if extra:
        p.update(extra)
    return p


def _fetch_events() -> list:
    """Fetch all greyhound events with caching."""
    now = time.time()
    if _cache["events"] is not None and (now - _cache["timestamp"]) < _cache["ttl"]:
        return _cache["events"]
    try:
        r = requests.get(f"{BASE_URL}/categories/{CATEGORY}/events", params=_params(), timeout=20)
        r.raise_for_status()
        events = r.json().get("events", {}).get("event", [])
        _cache["events"] = events
        _cache["timestamp"] = now
        log.info(f"LADS: fetched {len(events)} greyhound events")
        return events
    except Exception as e:
        log.warning(f"LADS events fetch failed: {e}")
        return _cache["events"] or []


def _fetch_event_detail(event_id) -> dict | None:
    try:
        r = requests.get(f"{BASE_URL}/events/{event_id}", params=_params({"expand": "selection"}), timeout=5)
        r.raise_for_status()
        return r.json().get("event", {})
    except Exception:
        return None


def _clean(name: str) -> str:
    return name.strip("|").strip()


def _parse_event_name(name: str):
    import re
    clean = _clean(name)
    m = re.match(r"(\d{1,2}:\d{2})\s+(.+)", clean)
    if m:
        return m.group(2).strip(), m.group(1)
    return clean, ""


def fetch_greyhound_races(race_date: str) -> list:
    """
    Fetch AU greyhound races for a date from Ladbrokes One API.
    Returns list of race dicts with track, race_number, start_time, runners (with lads_win).
    """
    all_events = _fetch_events()
    target = date.fromisoformat(race_date)

    # Filter AU greyhounds for target date (compare in Australian time)
    import pytz
    tz_aus = pytz.timezone("Australia/Sydney")

    # Known NZ tracks to exclude (Ladbrokes flags them as AU sometimes)
    NZ_TRACKS = {"addington", "cambridge", "christchurch", "manawatu", "wanganui", "palmerston north", "ascot park nz"}

    au_events = []
    for ev in all_events:
        flags = ev.get("typeFlagCode", "")
        if "AU" not in flags:
            continue
        dt_str = ev.get("eventDateTime", "")
        if not dt_str:
            continue
        # Check track name for NZ
        ev_name = ev.get("eventName", "").strip("|").strip().lower()
        if any(nz in ev_name for nz in NZ_TRACKS):
            continue
        try:
            ev_dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            ev_aus_date = ev_dt.astimezone(tz_aus).date()
            if ev_aus_date != target:
                continue
        except (ValueError, TypeError):
            continue
        au_events.append(ev)

    log.info(f"LADS: {len(au_events)} AU greyhound events for {race_date}")

    # Group by typeKey (track/meeting)
    groups = {}
    for ev in au_events:
        tk = ev.get("meta", {}).get("typeKey", 0)
        groups.setdefault(tk, []).append(ev)

    for tk in groups:
        groups[tk].sort(key=lambda e: e.get("eventDateTime", ""))

    # Build races + fetch details concurrently
    races = []
    tasks = []

    for tk, evs in groups.items():
        for idx, ev in enumerate(evs):
            eid = ev.get("eventKey")
            track, _ = _parse_event_name(ev.get("eventName", ""))
            race = {
                "track": track,
                "race_number": idx + 1,
                "start_time": ev.get("eventDateTime", ""),
                "distance": "",
                "runners": [],
                "venue_slug": track.lower().replace(" ", "-").replace("'", ""),
                "race_id": eid,
                "race_status": _map_status(ev.get("eventStatusCode", "")),
            }
            races.append(race)
            tasks.append((race, eid))

    # Fetch selections concurrently
    def _enrich(task):
        race, eid = task
        detail = _fetch_event_detail(eid)
        if detail:
            _extract_runners(race, detail)

    with ThreadPoolExecutor(max_workers=40) as pool:
        list(pool.map(_enrich, tasks))

    return races


def _map_status(code: str) -> str:
    code = code.upper()
    if code in ("FI", "RE", "C"):
        return "finished"
    if code in ("A", "AB"):
        return "abandoned"
    return "open"


def _extract_runners(race, detail):
    markets = detail.get("markets", {}).get("market", [])
    win_market = None
    for m in markets:
        mname = _clean(m.get("marketName", "")).lower()
        if "win" in mname or "each way" in mname:
            win_market = m
            break
    if not win_market and markets:
        win_market = markets[0]
    if not win_market:
        return

    sels = win_market.get("selections", {}).get("selection", [])
    for sel in sels:
        name = _clean(sel.get("selectionName", ""))
        name_upper = name.upper()
        number = sel.get("runnerNumber", "")
        status_code = sel.get("selectionStatus", "")

        # Skip unnamed favourite
        if "UNNAMED" in name_upper and "FAVOURITE" in name_upper:
            continue

        # Classify — LADS uses selectionStatus for scratching
        if "VACANT" in name_upper:
            runner_status = "vacant"
        elif ("N/R" in name_upper or
              "NON-RUNNER" in name_upper or
              "NON RUNNER" in name_upper or
              status_code == "Suspended"):
            runner_status = "nr"
        else:
            runner_status = "valid"

        # Extract LADS price
        lads_win = None
        if runner_status == "valid":
            price = sel.get("currentPrice", {})
            if price:
                dec = price.get("priceDec")
                if dec and float(dec) > 1.01:
                    lads_win = float(dec)
                else:
                    num = price.get("priceNum")
                    den = price.get("priceDen")
                    if num is not None and den is not None and den > 0:
                        val = round((num / den) + 1, 2)
                        if val > 1.01:
                            lads_win = val

        race["runners"].append({
            "name": name,
            "number": str(number),
            "barrier": str(number),
            "lads_win": lads_win,
            "status": runner_status,
        })

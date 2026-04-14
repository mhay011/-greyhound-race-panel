"""
Adapter: Ladbrokes AU Greyhounds via the One API.

All network I/O is isolated here. Returns raw dicts straight from the API.
Env vars:
    LADS_AU_API_BASE_URL  (default: https://one-api.ladbrokes.com/v4/sportsbook-api)
    LADS_AU_API_TOKEN      (default: built-in public key)
"""
import os
import logging
import requests
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor

import pytz

log = logging.getLogger(__name__)

BASE_URL = os.environ.get(
    "LADS_AU_API_BASE_URL",
    "https://one-api.ladbrokes.com/v4/sportsbook-api",
)
API_KEY = os.environ.get(
    "LADS_AU_API_TOKEN",
    os.environ.get("LADBROKES_API_KEY", "LD0d4820596d8040c8b242b13771cf2449"),
)
CATEGORY = 19  # Greyhounds
TIMEOUT = 15
POOL_SIZE = 30

NZ_TRACKS = {
    "addington", "cambridge", "christchurch", "manawatu",
    "wanganui", "palmerston north", "ascot park nz",
}


def _params(extra=None):
    p = {"locale": "en-GB", "api-key": API_KEY}
    if extra:
        p.update(extra)
    return p


def _fetch_events():
    """Fetch all greyhound events from the One API."""
    r = requests.get(
        f"{BASE_URL}/categories/{CATEGORY}/events",
        params=_params(),
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json().get("events", {}).get("event", [])


def _fetch_event_detail(event_id):
    """Fetch a single event with selections expanded."""
    r = requests.get(
        f"{BASE_URL}/events/{event_id}",
        params=_params({"expand": "selection"}),
        timeout=5,
    )
    r.raise_for_status()
    return r.json().get("event", {})


def fetch_greyhound_races(race_date: str | None = None) -> list[dict]:
    """
    Fetch AU greyhound races for *race_date* (YYYY-MM-DD, default today).

    Returns the raw race dicts with runners and LADS prices attached.
    Raises on total failure; individual event-detail failures are swallowed
    (the race just has an empty runner list).
    """
    if race_date is None:
        race_date = date.today().isoformat()
    target = date.fromisoformat(race_date)
    tz_aus = pytz.timezone("Australia/Sydney")

    all_events = _fetch_events()

    au_events = []
    for ev in all_events:
        if "AU" not in ev.get("typeFlagCode", ""):
            continue
        dt_str = ev.get("eventDateTime", "")
        if not dt_str:
            continue
        ev_name = ev.get("eventName", "").strip("|").strip().lower()
        if any(nz in ev_name for nz in NZ_TRACKS):
            continue
        try:
            ev_dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            if ev_dt.astimezone(tz_aus).date() != target:
                continue
        except (ValueError, TypeError):
            continue
        au_events.append(ev)

    log.info("LADS AU checker: %d AU greyhound events for %s", len(au_events), race_date)

    # Group by typeKey (track/meeting) and assign race numbers
    groups: dict[int, list] = {}
    for ev in au_events:
        tk = ev.get("meta", {}).get("typeKey", 0)
        groups.setdefault(tk, []).append(ev)
    for tk in groups:
        groups[tk].sort(key=lambda e: e.get("eventDateTime", ""))

    races = []
    tasks = []
    for _tk, evs in groups.items():
        for idx, ev in enumerate(evs):
            import re
            raw_name = ev.get("eventName", "").strip("|").strip()
            m = re.match(r"(\d{1,2}:\d{2})\s+(.+)", raw_name)
            track = m.group(2).strip() if m else raw_name
            race = {
                "track": track,
                "race_number": idx + 1,
                "start_time": ev.get("eventDateTime", ""),
                "event_id": ev.get("eventKey"),
                "runners": [],
                "raw_event": ev,
            }
            races.append(race)
            tasks.append((race, ev.get("eventKey")))

    # Fetch selections concurrently
    def _enrich(task):
        race, eid = task
        try:
            detail = _fetch_event_detail(eid)
            _extract_runners(race, detail)
        except Exception as exc:
            log.warning("LADS AU: detail fetch failed for %s: %s", eid, exc)

    with ThreadPoolExecutor(max_workers=POOL_SIZE) as pool:
        list(pool.map(_enrich, tasks))

    return races


def _extract_runners(race, detail):
    """Parse selections from event detail into race['runners']."""
    markets = detail.get("markets", {}).get("market", [])
    win_market = None
    for m in markets:
        mname = m.get("marketName", "").strip("|").strip().lower()
        if "win" in mname or "each way" in mname:
            win_market = m
            break
    if not win_market and markets:
        win_market = markets[0]
    if not win_market:
        return

    # Extract EW terms from market if present
    ew_terms = None
    ew_num = win_market.get("eachWayFactorNum")
    ew_den = win_market.get("eachWayFactorDen")
    ew_places = win_market.get("eachWayPlaces")
    if ew_num is not None and ew_den is not None and ew_places is not None:
        try:
            ew_terms = {
                "ew_factor_num": int(ew_num),
                "ew_factor_den": int(ew_den),
                "ew_places": int(ew_places),
            }
        except (ValueError, TypeError):
            pass
    race["ew_terms"] = ew_terms

    sels = win_market.get("selections", {}).get("selection", [])
    for sel in sels:
        name = sel.get("selectionName", "").strip("|").strip()
        name_upper = name.upper()
        number = sel.get("runnerNumber", "")
        status_code = sel.get("selectionStatus", "")

        if "UNNAMED" in name_upper and "FAVOURITE" in name_upper:
            continue

        if "VACANT" in name_upper:
            runner_status = "vacant"
        elif ("N/R" in name_upper or "NON-RUNNER" in name_upper
              or "NON RUNNER" in name_upper or status_code == "Suspended"):
            runner_status = "nr"
        else:
            runner_status = "valid"

        # Price extraction
        num_price = None
        den_price = None
        decimal_price = None
        if runner_status == "valid":
            price = sel.get("currentPrice", {})
            if price:
                pn = price.get("priceNum")
                pd = price.get("priceDen")
                dec = price.get("priceDec")
                if pn is not None and pd is not None:
                    try:
                        num_price = float(pn)
                        den_price = float(pd)
                    except (ValueError, TypeError):
                        pass
                if dec is not None:
                    try:
                        decimal_price = float(dec)
                    except (ValueError, TypeError):
                        pass
                # Compute decimal from fractional if missing
                if decimal_price is None and num_price and den_price and den_price > 0:
                    decimal_price = round((num_price / den_price) + 1, 2)
                # Compute fractional from decimal if missing
                if num_price is None and decimal_price and decimal_price > 1:
                    num_price = round(decimal_price - 1, 4)
                    den_price = 1.0

        race["runners"].append({
            "name": name,
            "number": str(number),
            "status": runner_status,
            "num_price": num_price,
            "den_price": den_price,
            "decimal_price": decimal_price,
        })

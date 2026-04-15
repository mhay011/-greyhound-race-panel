"""
Service for fetching AU greyhound odds from Unibet UK GraphQL API.

Flow:
1. LobbyMeetingListQuery → all meetings with event keys
2. Filter to AUS greyhounds
3. EventQuery per race → runner names + FixedWin prices

No auth required. Works globally (not geo-restricted to AU).
"""
import logging
import json
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger(__name__)

BASE_URL = "https://rsa.unibet.co.uk/api/v1/graphql"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Origin": "https://www.unibet.co.uk",
    "Referer": "https://www.unibet.co.uk/racing",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

LOBBY_HASH = "8c866a910971369c3114923ef7559f135a1347421e1cb19ea38da80c5a6c7db8"
EVENT_HASH = "398826cdf61fa578855ad6e42998d2637c622adcb39ade2ab8481a7aaf5d08f1"
TIMEOUT = 10
POOL_SIZE = 15

# Cache lobby data per date (cleared on redeploy)
_lobby_cache = {"date": None, "data": None}


def _fetch_lobby(race_date: str) -> list:
    """Fetch all meetings for a date via LobbyMeetingListQuery."""
    # Unibet uses AU racing day: previous day 16:00 UTC to current day 16:00 UTC
    # This covers the full AU racing day (2am AEST to 2am AEST next day)
    from datetime import date as d, timedelta as td
    dt = d.fromisoformat(race_date)
    prev = dt - td(days=1)
    start_dt = f"{prev.isoformat()}T16:00:00.000Z"
    end_dt = f"{dt.isoformat()}T16:00:00.000Z"

    variables = json.dumps({
        "countryCodes": [],
        "clientCountryCode": "GB",
        "startDateTime": start_dt,
        "endDateTime": end_dt,
        "virtualStartDateTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "virtualEndDateTime": (datetime.utcnow() + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "isRenderingVirtual": True,
        "fetchTRC": False,
        "raceTypes": ["T", "H", "G"],
    })
    extensions = json.dumps({
        "persistedQuery": {"version": 1, "sha256Hash": LOBBY_HASH}
    })

    params = {
        "operationName": "LobbyMeetingListQuery",
        "variables": variables,
        "extensions": extensions,
    }

    try:
        r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            log.warning(f"Unibet lobby: HTTP {r.status_code}")
            return []
        data = r.json()
        viewer = data.get("data", {}).get("viewer", {})
        meetings = viewer.get("meetings", [])
        return meetings
    except Exception as e:
        log.warning(f"Unibet lobby fetch failed: {e}")
        return []


def _fetch_event(event_key: str) -> dict | None:
    """Fetch a single race event via EventQuery."""
    variables = json.dumps({
        "clientCountryCode": "GB",
        "eventKey": event_key,
        "fetchTRC": False,
    })
    extensions = json.dumps({
        "persistedQuery": {"version": 1, "sha256Hash": EVENT_HASH}
    })

    params = {
        "operationName": "EventQuery",
        "variables": variables,
        "extensions": extensions,
    }

    try:
        r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            log.warning(f"Unibet event {event_key}: HTTP {r.status_code}")
            return None
        data = r.json()
        event = data.get("data", {}).get("event")
        if event and event.get("competitors"):
            return event
        log.warning(f"Unibet event {event_key}: no competitors in response")
        return None
    except Exception:
        return None


def _extract_runners(event_data: dict) -> list:
    """Extract runner names and FixedWin prices from event response."""
    runners = []
    
    # Try multiple paths for competitors
    competitors = (event_data.get("competitors") or 
                   event_data.get("Competitors") or
                   event_data.get("event", {}).get("competitors") or
                   [])

    for comp in competitors:
        name = comp.get("name", comp.get("Name", ""))
        status = comp.get("status", comp.get("Status", ""))
        start_pos = comp.get("startPos", comp.get("StartPos", comp.get("startPosition")))

        unibet_win = None
        prices = comp.get("prices", comp.get("Prices", []))
        for pg in prices:
            bet_type = pg.get("betType", pg.get("BetType", ""))
            if bet_type in ("FixedWin", "FXD"):
                # Look for price in flucs
                flucs = pg.get("flucs", pg.get("Flucs", []))
                for fluc in flucs:
                    pt = fluc.get("productType", fluc.get("ProductType", ""))
                    if pt in ("Current", "Flux", "Max"):
                        p = fluc.get("price", fluc.get("Price"))
                        if p and p > 1.0:
                            unibet_win = p
                            break
                # Fallback to top-level price in the price group
                if unibet_win is None:
                    p = pg.get("price", pg.get("Price"))
                    if p and p > 1.0:
                        unibet_win = p
                break
        
        # Also check top-level price field on competitor
        if unibet_win is None:
            top_price = comp.get("price", comp.get("Price"))
            if top_price and isinstance(top_price, (int, float)) and top_price > 1.0:
                unibet_win = top_price

        # Determine status
        if status in ("Vacant Box", "VacantBox"):
            r_status = "vacant"
        elif status in ("Non-Runner", "NonRunner", "Scratched"):
            r_status = "nr"
        elif status in ("Starter", "Active", "Open"):
            r_status = "valid"
        else:
            r_status = "valid" if unibet_win else "nr"

        runners.append({
            "name": name,
            "number": str(start_pos) if start_pos else "",
            "unibet_win": unibet_win,
            "status": r_status,
        })

    return runners

    return runners


def fetch_unibet_meetings(race_date: str) -> dict:
    """
    Fetch all AU greyhound meetings and their event keys from Unibet.
    Cached per date.
    """
    if _lobby_cache["date"] == race_date and _lobby_cache["data"] is not None:
        return _lobby_cache["data"]

    all_meetings = _fetch_lobby(race_date)
    log.info(f"Unibet lobby returned {len(all_meetings)} total meetings")
    for m in all_meetings[:3]:
        log.info(f"  Sample meeting: key={m.get('meetingKey','?')} country={m.get('countryCode','?')} type={m.get('raceType','?')} name={m.get('name','?')}")

    au_greyhounds = {}
    for meeting in all_meetings:
        meeting_key = meeting.get("meetingKey", "")
        # Check for AUS in meeting key (format: YYYYMMDDNNNN.G.AUS.track)
        # Also check countryCode field as fallback
        country = meeting.get("countryCode", "")
        is_aus = ".AUS." in meeting_key or country in ("AUS", "AU")
        race_type = meeting.get("raceType", "")
        is_greyhound = race_type == "G" or ".G." in meeting_key

        if not is_aus or not is_greyhound:
            continue

        name = meeting.get("name", "")
        meeting_key = meeting.get("meetingKey", "")
        slug = name.lower().replace(" ", "_").replace("'", "")

        events = []
        for ev in meeting.get("events", []):
            events.append({
                "event_key": ev.get("eventKey", ""),
                "sequence": ev.get("sequence", 0),
                "status": ev.get("status", ""),
                "start_time": ev.get("eventDateTimeUtc", ""),
            })

        au_greyhounds[slug] = {
            "name": name,
            "meeting_key": meeting_key,
            "events": sorted(events, key=lambda e: e["sequence"]),
        }

    log.info(f"Unibet: {len(au_greyhounds)} AU greyhound meetings found")
    _lobby_cache["date"] = race_date
    _lobby_cache["data"] = au_greyhounds
    return au_greyhounds


def fetch_unibet_odds_for_race(race_date: str, track_slug: str, race_number: int) -> list | None:
    """
    Fetch Unibet odds for a single race.
    Uses the lobby to find the correct event key, then fetches prices.
    """
    meetings = fetch_unibet_meetings(race_date)

    # Try to match track slug
    slug_clean = track_slug.lower().replace(" ", "_").replace("-", "_").replace("'", "")
    meeting = meetings.get(slug_clean)

    # Fuzzy match if exact slug doesn't work
    if not meeting:
        for key, m in meetings.items():
            if slug_clean in key or key in slug_clean:
                meeting = m
                break
            # Also try matching the meeting name
            m_name = m["name"].lower().replace(" ", "_")
            if slug_clean in m_name or m_name in slug_clean:
                meeting = m
                break

    if not meeting:
        return None

    # Find the event for this race number
    event_key = None
    for ev in meeting["events"]:
        if ev["sequence"] == race_number:
            event_key = ev["event_key"]
            break

    if not event_key:
        return None

    event_data = _fetch_event(event_key)
    if not event_data:
        return None

    return _extract_runners(event_data)


def match_unibet_odds_to_runner(runner_name: str, runner_number: str, unibet_runners: list) -> float | None:
    """Match a runner to Unibet odds by name or number."""
    if not unibet_runners:
        return None

    name_lower = runner_name.strip().lower()
    for ur in unibet_runners:
        if ur.get("name", "").strip().lower() == name_lower:
            return ur.get("unibet_win")
        if runner_number and str(ur.get("number")) == str(runner_number):
            return ur.get("unibet_win")

    return None

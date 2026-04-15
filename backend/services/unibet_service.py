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

# Cache lobby data per date
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
        "virtualStartDateTime": start_dt,
        "virtualEndDateTime": end_dt,
        "isRenderingVirtual": True,
        "fetchTRC": False,
        "raceTypes": ["G"],
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
        meetings = data.get("data", {}).get("meetingList", [])
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
            return None
        data = r.json()
        return data.get("data", {}).get("event")
    except Exception:
        return None


def _extract_runners(event_data: dict) -> list:
    """Extract runner names and FixedWin prices from event response."""
    runners = []
    competitors = event_data.get("competitors", [])

    for comp in competitors:
        name = comp.get("name", "")
        status = comp.get("status", "")
        start_pos = comp.get("startPos")

        unibet_win = None
        prices = comp.get("prices", [])
        for pg in prices:
            if pg.get("betType") == "FixedWin":
                # Look for Current price in flucs
                for fluc in pg.get("flucs", []):
                    if fluc.get("productType") == "Current":
                        unibet_win = fluc.get("price")
                        break
                # Fallback to top-level price
                if unibet_win is None:
                    unibet_win = pg.get("price")
                break

        runners.append({
            "name": name,
            "number": str(start_pos) if start_pos else "",
            "unibet_win": unibet_win,
            "status": "valid" if status == "Starter" else "nr",
        })

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

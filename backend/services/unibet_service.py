"""
Service for fetching AU greyhound odds from Unibet UK GraphQL API.

Endpoint: https://rsa.unibet.co.uk/api/v1/graphql
No auth required. Works globally (not geo-restricted to AU).

Event key format: {YYYYMMDD}0{sequence}.G.AUS.{track_slug}.{race_number}
Example: 20260415010.G.AUS.sale.1

Price data includes:
- FixedWin (FXD) with Current, Max, Last prices
- Runner name, status, trainer, form
"""
import logging
import json
import re
import requests
from datetime import date
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

# Persisted query hash from Unibet's frontend
EVENT_QUERY_HASH = "22398826cd61fa578855ad6e42998d2637c622adcb39ade2ab8481a7aaf5d08f1"
TIMEOUT = 10
POOL_SIZE = 10


def _build_event_key(race_date: str, track_slug: str, race_number: int) -> str:
    """
    Build Unibet event key.
    Format: {YYYYMMDD}010.G.AUS.{track_slug}.{race_number}
    """
    d = race_date.replace("-", "")
    slug = track_slug.lower().replace(" ", "_").replace("-", "_").replace("'", "")
    return f"{d}010.G.AUS.{slug}.{race_number}"


def _fetch_event(event_key: str) -> dict | None:
    """Fetch a single race event from Unibet GraphQL API."""
    variables = json.dumps({
        "clientCountryCode": "GB",
        "eventKey": event_key,
    })
    extensions = json.dumps({
        "persistedQuery": {
            "version": 1,
            "sha256Hash": EVENT_QUERY_HASH,
        }
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
    except Exception as e:
        log.warning(f"Unibet fetch failed for {event_key}: {e}")
        return None


def _extract_prices(event_data: dict) -> list:
    """Extract runner prices from Unibet event response."""
    runners = []
    competitors = event_data.get("competitors", [])

    for comp in competitors:
        name = comp.get("name", "")
        status = comp.get("status", "")
        start_pos = comp.get("startPos")

        # Extract FixedWin price
        unibet_win = None
        prices = comp.get("prices", [])
        for price_group in prices:
            if price_group.get("betType") == "FixedWin":
                flucs = price_group.get("flucs", [])
                for fluc in flucs:
                    if fluc.get("productType") == "Current":
                        unibet_win = fluc.get("price")
                        break
                # Fallback to top-level price
                if unibet_win is None:
                    unibet_win = price_group.get("price")

        runners.append({
            "name": name,
            "number": str(start_pos) if start_pos else "",
            "unibet_win": unibet_win,
            "status": "valid" if status == "Starter" else "nr",
        })

    return runners


def fetch_unibet_odds_for_race(race_date: str, track_slug: str, race_number: int) -> list | None:
    """
    Fetch Unibet UK odds for a single AU greyhound race.

    Returns list of {name, number, unibet_win, status} or None if unavailable.
    """
    event_key = _build_event_key(race_date, track_slug, race_number)
    event_data = _fetch_event(event_key)
    if not event_data:
        # Try alternative slug formats
        alt_slugs = [
            track_slug.lower().replace(" ", ""),
            track_slug.lower().replace(" ", "_"),
            track_slug.lower().replace(" ", "-"),
        ]
        for slug in alt_slugs:
            alt_key = f"{race_date.replace('-', '')}010.G.AUS.{slug}.{race_number}"
            event_data = _fetch_event(alt_key)
            if event_data:
                break

    if not event_data:
        return None

    return _extract_prices(event_data)


def fetch_unibet_odds_for_meeting(race_date: str, track_slug: str, num_races: int) -> dict:
    """
    Fetch Unibet odds for all races at a meeting.

    Returns dict: {race_number: [runner_odds_list]}
    """
    results = {}

    def _fetch_race(rnum):
        odds = fetch_unibet_odds_for_race(race_date, track_slug, rnum)
        return rnum, odds

    with ThreadPoolExecutor(max_workers=POOL_SIZE) as pool:
        futures = [pool.submit(_fetch_race, r) for r in range(1, num_races + 1)]
        for f in futures:
            rnum, odds = f.result()
            if odds:
                results[rnum] = odds

    log.info(f"Unibet: {track_slug} - got odds for {len(results)}/{num_races} races")
    return results


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

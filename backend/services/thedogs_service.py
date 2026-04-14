"""
Service for fetching AU Greyhound schedule from thedogs.com.au.

Scrapes the racecards page for meeting list, then each meeting page
for race times and runner fields (box, name, trainer, form, best time).
Concurrent fetching for speed.
"""
import re
import logging
import requests
from html import unescape as html_unescape
from datetime import date
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger(__name__)

BASE_URL = "https://www.thedogs.com.au"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
TIMEOUT = 12
POOL_SIZE = 10


def fetch_meetings(target_date: str | None = None) -> list[dict]:
    """Fetch all scheduled meetings from the racecards page."""
    if target_date is None:
        target_date = date.today().isoformat()
    try:
        r = requests.get(f"{BASE_URL}/racing/racecards", headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"thedogs: racecards fetch failed: {e}")
        return []

    matches = re.findall(
        r'href="(/racing/([^/]+)/(\d{4}-\d{2}-\d{2})\?trial=false)"', r.text
    )
    seen = set()
    meetings = []
    for url_path, slug, meet_date in matches:
        key = (slug, meet_date)
        if key in seen:
            continue
        seen.add(key)
        meetings.append({
            "track": slug.replace("-", " ").title(),
            "slug": slug,
            "date": meet_date,
            "url": f"{BASE_URL}{url_path}",
        })
    log.info(f"thedogs: {len(meetings)} meetings found")
    return meetings


def _parse_meeting_html(html: str) -> list[dict]:
    """Parse a meeting page HTML into race dicts."""
    race_times = []
    seen_times = set()
    for t in re.findall(r'data-race-box="([^"]+)"', html):
        if t not in seen_times:
            seen_times.add(t)
            race_times.append(t)

    tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)

    races = []
    for i, table_html in enumerate(tables):
        race_num = i + 1
        start_time = race_times[i] if i < len(race_times) else ""

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL | re.IGNORECASE)
        runners = []
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
            if len(cells) < 3:
                continue

            box_match = re.search(r'(\d+)', cells[0])
            box = box_match.group(1) if box_match else ""

            # Replace HTML tags with | delimiter to find name boundaries
            # Pattern: ||Name||Colour Sex|||||T: Trainer|
            cell_delim = html_unescape(re.sub(r'<[^>]+>', '|', cells[1]).strip())
            name_cell = html_unescape(re.sub(r'<[^>]+>', ' ', cells[1]).strip())
            
            # Extract name from between first || pair
            parts = [p.strip() for p in cell_delim.split('|') if p.strip()]
            if parts:
                name = parts[0]
                # Clean up any extra whitespace within name
                name = re.sub(r'\s+', ' ', name).strip()
            else:
                name = name_cell.split('T:')[0].strip() if 'T:' in name_cell else name_cell.strip()

            trainer = html_unescape(re.sub(r'<[^>]+>', '', cells[2]).strip())
            form = re.sub(r'<[^>]+>', '', cells[3]).strip() if len(cells) > 3 else ""
            best_time = re.sub(r'<[^>]+>', '', cells[4]).strip() if len(cells) > 4 else ""

            # Detect scratched / vacant
            is_scratched = bool(re.search(r'scratched|line-through', row, re.IGNORECASE))
            if re.search(r'\(SCR\)', name_cell, re.IGNORECASE):
                is_scratched = True
                name = re.sub(r'\s*\(SCR\)\s*', '', name).strip()
            is_vacant = bool(re.search(r'vacant\s*box', name_cell, re.IGNORECASE))
            if is_vacant:
                name = "VACANT"
                is_scratched = True

            if name and box:
                status = "vacant" if is_vacant else ("nr" if is_scratched else "valid")
                runners.append({
                    "box": box,
                    "name": name,
                    "trainer": trainer,
                    "form": form,
                    "best_time": best_time,
                    "status": status,
                })

        races.append({
            "race_number": race_num,
            "start_time": start_time,
            "runners": runners,
        })
    return races


def fetch_meeting_races(meeting_url: str) -> list[dict]:
    """Fetch races and runners for a single meeting."""
    try:
        r = requests.get(meeting_url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return _parse_meeting_html(r.text)
    except Exception as e:
        log.warning(f"thedogs: meeting fetch failed {meeting_url}: {e}")
        return []


def fetch_all_for_date(target_date: str | None = None) -> dict:
    """Fetch all meetings and their races for a date. Concurrent fetching."""
    if target_date is None:
        target_date = date.today().isoformat()

    meetings = fetch_meetings(target_date)
    day_meetings = [m for m in meetings if m["date"] == target_date]
    log.info(f"thedogs: {len(day_meetings)} meetings for {target_date}")

    # Fetch all meeting pages concurrently
    def _fetch(m):
        races = fetch_meeting_races(m["url"])
        return {
            "track": m["track"],
            "slug": m["slug"],
            "date": m["date"],
            "races": races,
        }

    with ThreadPoolExecutor(max_workers=POOL_SIZE) as pool:
        result = list(pool.map(_fetch, day_meetings))

    return {"date": target_date, "meetings": result}

"""
Service for fetching LADS, TAB, and LADS AU odds via the TAB Australia public API.

LADS = QLD jurisdiction (proxy for Ladbrokes UK — blocked from network)
TAB = NSW jurisdiction
LADS AU = VIC jurisdiction

Optimized: all HTTP calls are batched and run concurrently.
"""
import requests
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

log = logging.getLogger(__name__)

TAB_BASE = "https://api.beta.tab.com.au/v1/tab-info-service/racing/dates"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TIMEOUT = 6
POOL_SIZE = 30

_tab_cache = {"date": None, "data": None, "timestamp": 0, "ttl": 30}  # 30s cache — fresh prices every poll
_CACHE_FILE = os.path.join(os.path.dirname(__file__), "..", "_tab_cache.json")


def _load_disk_cache():
    """Load cache from disk if available."""
    try:
        import json
        if os.path.exists(_CACHE_FILE):
            with open(_CACHE_FILE, "r") as f:
                raw = json.load(f)
            # Convert string keys back to tuples
            odds = {}
            for k, v in raw.get("odds", {}).items():
                parts = k.split("|")
                if len(parts) == 2:
                    odds[(parts[0], int(parts[1]))] = v
            race_meta = {}
            for k, v in raw.get("race_meta", {}).items():
                parts = k.split("|")
                if len(parts) == 2:
                    race_meta[(parts[0], int(parts[1]))] = v
            data = {"odds": odds, "venue_names": raw.get("venue_names", {}), "race_meta": race_meta}
            _tab_cache["date"] = raw.get("date")
            _tab_cache["data"] = data
            _tab_cache["timestamp"] = raw.get("timestamp", 0)
            log.info(f"Loaded {len(odds)} races from disk cache")
    except Exception as e:
        log.warning(f"Failed to load disk cache: {e}")


def _save_disk_cache(race_date, data):
    """Save cache to disk."""
    try:
        import json
        raw = {
            "date": race_date,
            "timestamp": time.time(),
            "venue_names": data.get("venue_names", {}),
            "odds": {f"{k[0]}|{k[1]}": v for k, v in data.get("odds", {}).items()},
            "race_meta": {f"{k[0]}|{k[1]}": v for k, v in data.get("race_meta", {}).items()},
        }
        with open(_CACHE_FILE, "w") as f:
            json.dump(raw, f)
    except Exception as e:
        log.warning(f"Failed to save disk cache: {e}")


# Load disk cache on startup
import os
_load_disk_cache()

# QLD only — fastest, most reliable, earliest prices
JUR_MAP = {"QLD": "tab"}


def fetch_tab_odds_for_date(race_date: str, schedule_only: bool = False) -> dict:
    """Fetch TAB odds from QLD. schedule_only=True skips individual race detail fetches."""
    now = time.time()
    if (_tab_cache["date"] == race_date and
            _tab_cache["data"] is not None and
            (now - _tab_cache["timestamp"]) < _tab_cache["ttl"]):
        return _tab_cache["data"]

    result = {}

    # Phase 1: Fetch meetings from QLD only
    jur_meetings = {"QLD": _fetch_greyhound_meetings(race_date, "QLD")}
    log.info(f"QLD: {len(jur_meetings['QLD'])} greyhound meetings")

    # Phase 2: Collect race-list URLs + venue names
    races_list_tasks = []
    venue_names = {}
    for jur, meetings in jur_meetings.items():
        for m in meetings:
            vm = m.get("venueMnemonic", "")
            mname = m.get("meetingName", "")
            races_url = m.get("_links", {}).get("races", "")
            if vm and races_url:
                races_list_tasks.append((jur, vm, races_url))
                venue_names[vm] = mname

    # Phase 3: Fetch all race lists concurrently
    race_detail_tasks = []

    # Also store race metadata (start times)
    race_meta = {}  # (vm, rnum) → {"start_time": ...}

    def _fetch_races_list(task):
        jur, vm, url = task
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            items = []
            for race in r.json().get("races", []):
                rnum = race.get("raceNumber")
                race_self = race.get("_links", {}).get("self", "")
                if rnum and race_self:
                    items.append((jur, vm, rnum, race_self))
                    # Store start time (only need it once per race)
                    mk = (vm, rnum)
                    if mk not in race_meta:
                        status_val = race.get("raceStatus", "Open")
                        race_meta[mk] = {
                            "start_time": race.get("raceStartTime", ""),
                            "status": status_val,
                            "has_fixed_odds": race.get("hasFixedOdds", False),
                        }
                        if rnum == 1:
                            log.info(f"  Race meta {vm} R{rnum}: status={status_val}")
            return items
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=POOL_SIZE) as pool:
        for items in pool.map(_fetch_races_list, races_list_tasks):
            race_detail_tasks.extend(items)

    # Schedule-only mode: skip expensive race detail fetches, return structure only
    if schedule_only:
        # Build minimal result with empty runners (schedule + metadata)
        for task in race_detail_tasks:
            jur, vm, rnum, url = task
            prefix = JUR_MAP.get(jur, "tab")
            k = (vm, rnum)
            if k not in result:
                result[k] = {"tab_runners": [], "lads_au_runners": []}
        bundle = {"odds": result, "venue_names": venue_names, "race_meta": race_meta}
        # Cache briefly (10s) so full fetch can replace it
        _tab_cache["date"] = race_date
        _tab_cache["data"] = bundle
        _tab_cache["timestamp"] = time.time() - _tab_cache["ttl"] + 10
        log.info(f"Schedule-only: {len(result)} races (no prices)")
        return bundle

    # Phase 4: Fetch ALL race details concurrently
    def _fetch_race_detail(task):
        jur, vm, rnum, url = task
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            runners = []
            scratched = []

            # Get race-level scratchings list (runner numbers that are scratched)
            scratched_numbers = set()
            for s in data.get("scratchings", []):
                snum = str(s.get("runnerNumber", ""))
                if snum:
                    scratched_numbers.add(snum)

            for rr in data.get("runners", []):
                fo = rr.get("fixedOdds", {})
                name = rr.get("runnerName", "")
                number = str(rr.get("runnerNumber", ""))
                betting_status = (fo.get("bettingStatus") or "").lower()
                is_vacant = rr.get("vacantBox", False)
                win = fo.get("returnWin")

                # Detect non-runners: scratched, vacant, or 1.01 marker
                is_scratched = (
                    number in scratched_numbers or
                    is_vacant or
                    betting_status in ("losedeductions", "scratched", "removed", "latescratched")
                )

                # Only use scratchings list and bettingStatus for N/R detection
                # Do NOT use price thresholds — short prices (1.04) are real
                is_scratched = (
                    number in scratched_numbers or
                    is_vacant or
                    betting_status in ("losedeductions", "scratched", "removed", "latescratched")
                )

                if is_vacant:
                    scratched.append({"name": "VACANT", "number": number, "status": "vacant"})
                elif is_scratched:
                    scratched.append({"name": name, "number": number, "status": "nr"})
                elif win is not None and win > 0:
                    runners.append({
                        "name": name, "number": number,
                        "win": win, "place": fo.get("returnPlace"),
                    })
                else:
                    # Runner with no price yet (future race or finished) — valid runner
                    runners.append({
                        "name": name, "number": number,
                        "win": None, "place": None,
                    })
            return jur, vm, rnum, runners, scratched
        except Exception:
            return jur, vm, rnum, [], []

    with ThreadPoolExecutor(max_workers=POOL_SIZE) as pool:
        all_scratched = {}  # (vm, rnum) → {number: {name, number, status}}
        for jur, vm, rnum, runners, scratched in pool.map(_fetch_race_detail, race_detail_tasks):
            prefix = JUR_MAP.get(jur, "tab")
            k = (vm, rnum)
            if k not in result:
                result[k] = {"tab_runners": [], "lads_au_runners": []}
            # For TAB: keep whichever jurisdiction has more priced runners
            key = f"{prefix}_runners"
            existing = result[k].get(key, [])
            existing_priced = sum(1 for r in existing if r.get("win") is not None)
            new_priced = sum(1 for r in runners if r.get("win") is not None)
            if new_priced > existing_priced:
                result[k][key] = runners
            # Collect scratched from all jurisdictions
            if k not in all_scratched:
                all_scratched[k] = {}
            for s in scratched:
                all_scratched[k][s["number"]] = s

    # Remove scratched runners from runner lists
    for k, scr_map in all_scratched.items():
        if k not in result:
            continue
        scr_nums = set(scr_map.keys())
        for src in ("tab_runners", "lads_au_runners"):
            result[k][src] = [r for r in result[k].get(src, []) if r["number"] not in scr_nums]

    # Log what we got
    sample_k = list(result.keys())[:3] if result else []
    for k in sample_k:
        e = result[k]
        log.info(f"  {k}: tab={len(e.get('tab_runners',[]))} lads_au={len(e.get('lads_au_runners',[]))}")

    # Attach scratched info to result
    for k, scr in all_scratched.items():
        if k in result:
            result[k]["scratched"] = list(scr.values())

    # Preserve last known prices for finished races (from previous cache)
    old_data = _tab_cache.get("data", {}).get("odds", {}) if _tab_cache.get("data") else {}
    for k, entry in result.items():
        for src in ("tab_runners", "lads_au_runners"):
            new_runners = entry.get(src, [])
            # If runners exist but have no prices, try to fill from cache
            for nr in new_runners:
                if nr.get("win") is None and k in old_data:
                    old_runners = old_data[k].get(src, [])
                    for oldr in old_runners:
                        if oldr["number"] == nr["number"] and oldr.get("win"):
                            nr["win"] = oldr["win"]
                            nr["place"] = oldr.get("place")
                            break

    bundle = {"odds": result, "venue_names": venue_names, "race_meta": race_meta}
    if result:
        _tab_cache["date"] = race_date
        _tab_cache["data"] = bundle
        _tab_cache["timestamp"] = time.time()
        _save_disk_cache(race_date, bundle)
        log.info(f"Cached {len(result)} race entries for {race_date}")
    else:
        log.warning(f"No data fetched for {race_date}")

    return bundle


def _fetch_greyhound_meetings(race_date: str, jurisdiction: str) -> list:
    try:
        url = f"{TAB_BASE}/{race_date}/meetings?jurisdiction={jurisdiction}"
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return [m for m in r.json().get("meetings", [])
                if m.get("raceType") == "G" and m.get("location", "") not in ("NZ", "NZL")]
    except Exception as e:
        log.warning(f"TAB meetings fetch failed for {jurisdiction}: {e}")
        return []


def match_tab_odds_to_race(race: dict, tab_bundle: dict) -> dict:
    """Match TAB + LADS AU odds to a race's runners. Returns dict keyed by normalized name."""
    tab_data = tab_bundle.get("odds", {})
    venue_names = tab_bundle.get("venue_names", {})
    rnum = race.get("race_number")
    venue_slug = race.get("venue_slug", "")
    track = race.get("track", "").strip().lower()

    matched_key = _find_venue_match(track, venue_slug, rnum, tab_data, venue_names)
    if not matched_key:
        return {}

    entry = tab_data[matched_key]
    result = {}

    for src_key in ("tab_runners", "lads_au_runners"):
        prefix = src_key.replace("_runners", "")
        for tr in entry.get(src_key, []):
            name_key = tr["name"].strip().lower()
            num_key = tr["number"]
            if name_key and name_key not in result:
                result[name_key] = {"tab_win": None, "lads_au_win": None}
            if num_key and num_key not in result:
                result[num_key] = {"tab_win": None, "lads_au_win": None}
            if name_key:
                result[name_key][f"{prefix}_win"] = tr["win"]
            if num_key:
                result[num_key][f"{prefix}_win"] = tr["win"]

    # If LADS AU is empty (single jurisdiction mode), copy TAB prices
    has_lads_au = any(v.get("lads_au_win") for v in result.values())
    if not has_lads_au:
        for key, v in result.items():
            v["lads_au_win"] = v.get("tab_win")

    return result


def _find_venue_match(track, venue_slug, rnum, tab_data, venue_names):
    # Strategy 1: meeting name match
    for vm, mname in venue_names.items():
        ml = mname.strip().lower()
        if ml == track or ml in track or track in ml:
            key = (vm, rnum)
            if key in tab_data:
                return key

    # Strategy 2: slug match
    for vm, mname in venue_names.items():
        ms = mname.strip().lower().replace(" ", "-").replace("'", "")
        if venue_slug == ms or venue_slug.startswith(ms) or ms.startswith(venue_slug):
            key = (vm, rnum)
            if key in tab_data:
                return key

    # Strategy 3: mnemonic prefix
    for (vm, rn), _ in tab_data.items():
        if rn != rnum:
            continue
        vl = vm.lower()
        if venue_slug.startswith(vl) or vl.startswith(venue_slug[:3]):
            return (vm, rn)

    # Strategy 4: EXTRA meeting — race number exceeds main track, check EXTRA venue
    for vm, mname in venue_names.items():
        ml = mname.strip().lower()
        if "extra" not in ml:
            continue
        base = ml.replace("extra", "").strip()
        if base == track or base in track or track in base:
            # Find max race in main track to calculate extra race offset
            main_vm = None
            main_max = 0
            for vm2, mn2 in venue_names.items():
                mn2l = mn2.strip().lower()
                if mn2l == base or (mn2l == track and "extra" not in mn2l):
                    for (v, r), _ in tab_data.items():
                        if v == vm2 and r > main_max:
                            main_max = r
                    main_vm = vm2
                    break
            if main_max > 0 and rnum > main_max:
                extra_rnum = rnum - main_max
                key = (vm, extra_rnum)
                if key in tab_data:
                    return key

    log.warning(f"No match for {track} R{rnum} (slug={venue_slug})")
    return None

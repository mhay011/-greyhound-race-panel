"""Main Flask application for Australian Greyhound Race Panel.
Price sources: LADS (UK), TAB, Unibet UK
"""
import os, re, logging
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv
from datetime import date, datetime
import pytz

from services.ladbrokes_service import fetch_greyhound_races
from services.tab_service import fetch_tab_odds_for_date, match_tab_odds_to_race
from services.thedogs_service import fetch_all_for_date as fetch_thedogs
from services.unibet_service import fetch_unibet_odds_for_race, match_unibet_odds_to_runner
from services.calculations import (
    calculate_runner_probabilities,
    evaluate_each_way_value,
    get_ew_terms,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TZ_UK = pytz.timezone("Europe/London")
TZ_PH = pytz.timezone("Asia/Manila")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app = Flask(__name__, static_folder=STATIC_DIR)
CORS(app)

# Frozen prices for finished races: {(track, rnum, runner_num): {lads_win, tab_win}}
_frozen_prices = {}


@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/api/races", methods=["GET"])
def get_races():
    race_date = request.args.get("date", date.today().isoformat())

    # 1. TheDogs — fast schedule with complete runner fields
    dogs_data = None
    try:
        dogs_data = fetch_thedogs(race_date)
    except Exception as e:
        log.warning(f"TheDogs fetch failed: {e}")

    # 2. TAB — full fetch for prices and race status
    tab_bundle = {"odds": {}, "venue_names": {}, "race_meta": {}}
    try:
        tab_bundle = fetch_tab_odds_for_date(race_date)
    except Exception as e:
        log.warning(f"TAB fetch failed: {e}")

    # Build races from TheDogs if available, otherwise fall back to TAB, then Ladbrokes
    if dogs_data and dogs_data.get("meetings"):
        races = _build_races_from_thedogs(dogs_data, tab_bundle)
        log.info(f"Built {len(races)} races from TheDogs schedule")
    elif tab_bundle.get("odds"):
        races = _build_races_from_tab(tab_bundle)
        log.info(f"Fallback: built {len(races)} races from TAB")
    else:
        # Final fallback: Ladbrokes API (always reachable)
        try:
            lads_races = fetch_greyhound_races(race_date)
            if lads_races:
                races = lads_races
                log.info(f"Ladbrokes fallback: built {len(races)} races")
            else:
                return jsonify({"error": "No race data available."}), 502
        except Exception as e:
            log.warning(f"Ladbrokes fallback also failed: {e}")
            return jsonify({"error": "No race data available."}), 502

    # LADS scratchings skipped on initial load for speed
    # Applied via /api/prices polling instead

    enriched_races = []
    for race in races:
        tab_odds_map = match_tab_odds_to_race(race, tab_bundle)
        track = race.get("track", "?")
        rnum = race.get("race_number", "?")
        tab_c = sum(1 for v in tab_odds_map.values() if v.get("tab_win")) if tab_odds_map else 0

        # Fetch Unibet UK odds for this race
        venue_slug = race.get("venue_slug", track.lower().replace(" ", "_"))
        unibet_runners = None
        try:
            unibet_runners = fetch_unibet_odds_for_race(race_date, venue_slug, rnum)
        except Exception as e:
            log.warning(f"Unibet fetch failed {track} R{rnum}: {e}")

        ub_c = len([r for r in (unibet_runners or []) if r.get("unibet_win")]) if unibet_runners else 0
        log.info(f"{track} R{rnum}: map={len(tab_odds_map)} tab={tab_c} unibet={ub_c}")
        times = _convert_race_time(race.get("start_time", ""))

        all_runners = []
        valid_runners = []
        raw_runners = race.get("runners", [])
        valid_count = sum(1 for r in raw_runners if r.get("status", "valid") == "valid")
        ew_fraction, places_paid = get_ew_terms(valid_count)

        for runner in raw_runners:
            status = runner.get("status", "valid")
            number = runner.get("number", "")

            if status == "valid":
                race_st = race.get("race_status", "open")
                odds = _build_runner_odds(runner, tab_odds_map, race_st, track, rnum)
                # Pass through lads_win if present on runner (Ladbrokes fallback)
                if runner.get("lads_win") and not odds.get("lads_win"):
                    odds["lads_win"] = runner["lads_win"]
                # Add Unibet price
                ub_price = match_unibet_odds_to_runner(
                    runner.get("name", ""), str(number), unibet_runners or []
                )
                odds["unibet_win"] = ub_price
                probs = calculate_runner_probabilities(odds, ew_fraction)
                entry = {
                    "name": runner.get("name", "Unknown"),
                    "number": number,
                    "barrier": runner.get("barrier", ""),
                    "status": "valid",
                    "lads_win": odds.get("lads_win"),
                    "lads_win_pct": probs.get("lads_win_pct"),
                    "lads_ew_pct": probs.get("lads_ew_pct"),
                    "tab_win": odds["tab_win"],
                    "tab_win_pct": probs["tab_win_pct"],
                    "tab_ew_pct": probs["tab_ew_pct"],
                    "unibet_win": ub_price,
                    "unibet_win_pct": probs.get("unibet_win_pct"),
                    "unibet_ew_pct": probs.get("unibet_ew_pct"),
                    "note": runner.get("_note", ""),
                }
                all_runners.append(entry)
                valid_runners.append(entry)
            else:
                all_runners.append({
                    "name": runner.get("name", status.upper()),
                    "number": number, "barrier": "", "status": status,
                    "lads_win": None, "lads_win_pct": None, "lads_ew_pct": None,
                    "tab_win": None, "tab_win_pct": None, "tab_ew_pct": None,
                    "unibet_win": None, "unibet_win_pct": None, "unibet_ew_pct": None,
                })

        all_runners.sort(key=lambda r: _safe_int(r.get("number")))

        # N/R detection for finished races: no TAB price (live or frozen) = was scratched
        # Open races: rely on TAB bettingStatus detection (LateScratched) + LADS scratchings
        race_st = race.get("race_status", "open")
        if race_st == "finished":
            new_valid = []
            for r in all_runners:
                if r["status"] == "valid" and r.get("tab_win") is None:
                    r["status"] = "nr"
                else:
                    if r["status"] == "valid":
                        new_valid.append(r)
            valid_runners = new_valid

        num_runners = len(valid_runners)
        ew_eval = evaluate_each_way_value(valid_runners, num_runners)

        enriched_races.append({
            "track": track,
            "race_number": race.get("race_number", ""),
            "start_time": race.get("start_time", ""),
            "uk_time": times["uk_time"],
            "ph_time": times["ph_time"],
            "distance": race.get("distance", ""),
            "num_runners": num_runners,
            "runners": all_runners,
            "ew_places_paid": ew_eval["places_paid"],
            "ew_fraction": ew_fraction,
            "bad_each_way": ew_eval["bad_each_way"],
            "nearly_bad_ew": ew_eval.get("nearly_bad_ew", False),
            "bad_ew_reason": ew_eval.get("reason", ""),
            "total_ew_pct": ew_eval.get("total_ew_pct", 0),
            "threshold": ew_eval.get("threshold", 0),
            "venue_slug": race.get("venue_slug", ""),
            "race_status": race.get("race_status", "open"),
            "tab_live": any(r.get("tab_win") is not None for r in valid_runners),
            "totals": ew_eval.get("totals", {}),
        })

    enriched_races.sort(key=lambda r: (r["uk_time"], r["track"]))
    grouped = {}
    for race in enriched_races:
        t = race["track"]
        if t not in grouped:
            grouped[t] = {"track": t, "earliest_start": race["uk_time"], "races": []}
        grouped[t]["races"].append(race)

    tracks = sorted(grouped.values(), key=lambda t: t["earliest_start"])
    return jsonify({"date": race_date, "tracks": tracks})


@app.route("/api/prices", methods=["GET"])
def get_prices():
    """Lightweight endpoint — returns only prices + status for live races. No layout data."""
    race_date = request.args.get("date", date.today().isoformat())

    # TAB prices only
    tab_bundle = {"odds": {}, "venue_names": {}, "race_meta": {}}
    try:
        tab_bundle = fetch_tab_odds_for_date(race_date)
    except:
        pass

    # Build price updates per race
    updates = {}
    tab_data = tab_bundle.get("odds", {})
    venue_names = tab_bundle.get("venue_names", {})
    race_meta = tab_bundle.get("race_meta", {})

    # Detect extra meetings and count main track races for renumbering
    main_race_counts = {}  # base_track_upper → max race number
    extra_meetings = {}    # vm → base_track_upper
    
    for (vm, rnum), entry in tab_data.items():
        track_name = venue_names.get(vm, vm).upper()
        is_extra = "EXTRA" in track_name or "XTRA" in track_name
        if is_extra:
            base = re.sub(r'\s*EXTRA\s*', '', track_name, flags=re.IGNORECASE).strip()
            extra_meetings[vm] = base
        else:
            if track_name not in main_race_counts or rnum > main_race_counts[track_name]:
                main_race_counts[track_name] = rnum

    for (vm, rnum), entry in tab_data.items():
        track_name = venue_names.get(vm, vm)
        track_upper = track_name.upper()
        
        # Renumber extra races to continue from main track
        actual_rnum = rnum
        if vm in extra_meetings:
            base = extra_meetings[vm]
            max_main = main_race_counts.get(base, 0)
            actual_rnum = max_main + rnum
            track_upper = base  # Use parent track name

        meta = race_meta.get((vm, rnum), {})
        tab_status = meta.get("status", "Open").lower()
        if tab_status in ("final", "paying", "resulted", "interim"):
            race_status = "finished"
        elif tab_status in ("abandoned", "deleted"):
            race_status = "abandoned"
        else:
            race_status = "open"

        runners = {}
        scratched_nums = set()

        # Collect scratched runners
        for s in entry.get("scratched", []):
            scratched_nums.add(s["number"])

        for src in ("tab_runners",):
            for tr in entry.get(src, []):
                name = tr["name"].strip().lower()
                num = tr["number"]
                tw = tr.get("win")
                if num in scratched_nums:
                    continue  # Skip scratched
                if tw and tw > 1.01:
                    runners[num] = {"tab_win": tw}

        key = f"{track_upper}|{actual_rnum}"
        updates[key] = {"status": race_status, "runners": runners, "scratched": list(scratched_nums)}

    return jsonify({"updates": updates})


@app.route("/api/lads-test", methods=["GET"])
def lads_test():
    """Debug endpoint: show raw LADS API data + raw API response for AU greyhounds."""
    import json as jsonlib
    race_date = request.args.get("date", date.today().isoformat())
    results = []

    try:
        lads_races = fetch_greyhound_races(race_date)
        results.append(f"LADS: {len(lads_races)} AU races fetched")
        results.append("")

        for race in lads_races[:5]:
            track = race.get("track", "?")
            rnum = race.get("race_number", "?")
            status = race.get("race_status", "?")
            runners = race.get("runners", [])
            priced = [r for r in runners if r.get("lads_win")]
            results.append(f"📍 {track} R{rnum} [{status}]")
            results.append(f"   Runners: {len(runners)} total, {len(priced)} with LADS prices")
            for r in runners[:8]:
                lw = r.get("lads_win")
                st = r.get("status", "valid")
                price_str = f"{lw:.2f}" if lw else "SP"
                results.append(f"   {r.get('number'):>2}. {r.get('name','?'):25s} {st:8s} LADS: {price_str}")
            results.append("")

        # Raw API check for first race
        if lads_races:
            eid = lads_races[0].get("race_id")
            if eid:
                from services.ladbrokes_service import _params, BASE_URL
                import requests as req
                results.append("=== RAW API CHECK ===")
                results.append(f"Event ID: {eid}")
                try:
                    r = req.get(f"{BASE_URL}/events/{eid}", params=_params({"expand": "selection"}), timeout=8)
                    results.append(f"Status: {r.status_code}")
                    if r.status_code == 200:
                        ev = r.json().get("event", {})
                        markets = ev.get("markets", {}).get("market", [])
                        results.append(f"Markets: {len(markets)}")
                        if markets:
                            sels = markets[0].get("selections", {}).get("selection", [])
                            results.append(f"Selections: {len(sels)}")
                            for s in sels[:3]:
                                name = s.get("selectionName", "?")
                                cp = s.get("currentPrice", {})
                                results.append(f"  {name}: currentPrice={jsonlib.dumps(cp)[:200]}")
                except Exception as e:
                    results.append(f"Raw API error: {e}")
    except Exception as e:
        results.append(f"LADS ERROR: {e}")

    return jsonify({"output": "\n".join(results)})


@app.route("/api/thedogs", methods=["GET"])
def thedogs_schedule():
    """Fetch AU Greyhound schedule from thedogs.com.au."""
    race_date = request.args.get("date", date.today().isoformat())
    try:
        data = fetch_thedogs(race_date)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/unibet-test", methods=["GET"])
def unibet_test():
    """Debug: test Unibet UK API connectivity and data."""
    from services.unibet_service import BASE_URL, HEADERS, LOBBY_HASH
    import json as jlib
    import requests as req
    race_date = request.args.get("date", date.today().isoformat())
    results = []

    # Step 1: Raw HTTP test
    try:
        r = req.get(BASE_URL, params={"operationName": "test"}, headers=HEADERS, timeout=10)
        results.append(f"Raw API test: HTTP {r.status_code}, ct={r.headers.get('content-type','?')}")
        results.append(f"First 200 chars: {r.text[:200]}")
    except Exception as e:
        results.append(f"Raw API test FAILED: {e}")

    # Step 2: Raw lobby query with full response
    try:
        from datetime import date as d, timedelta as td
        dt = d.fromisoformat(race_date)
        prev = dt - td(days=1)
        start_dt = f"{prev.isoformat()}T16:00:00.000Z"
        end_dt = f"{dt.isoformat()}T16:00:00.000Z"

        variables = {
            "countryCodes": [],
            "clientCountryCode": "GB",
            "startDateTime": start_dt,
            "endDateTime": end_dt,
            "virtualStartDateTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "virtualEndDateTime": (datetime.utcnow() + td(hours=5)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "isRenderingVirtual": True,
            "fetchTRC": False,
            "raceTypes": ["T", "H", "G"],
        }
        extensions = {
            "persistedQuery": {"version": 1, "sha256Hash": LOBBY_HASH}
        }
        params = {
            "operationName": "LobbyMeetingListQuery",
            "variables": jlib.dumps(variables),
            "extensions": jlib.dumps(extensions),
        }

        results.append(f"\nLobby query date range: {start_dt} to {end_dt}")
        r2 = req.get(BASE_URL, params=params, headers=HEADERS, timeout=15)
        results.append(f"Lobby HTTP: {r2.status_code}, ct={r2.headers.get('content-type','?')}")

        if r2.status_code == 200 and "json" in r2.headers.get("content-type", ""):
            data = r2.json()
            meetings = data.get("data", {}).get("meetingList", [])
            results.append(f"Total meetings: {len(meetings)}")
            for m in meetings:
                mk = m.get("meetingKey", "?")
                name = m.get("name", "?")
                country = m.get("countryCode", "?")
                rtype = m.get("raceType", "?")
                n_events = len(m.get("events", []))
                is_aus = ".AUS." in mk
                results.append(f"  {'>>> ' if is_aus else ''}{name} | key={mk} | country={country} | type={rtype} | events={n_events}")
        else:
            results.append(f"Lobby response: {r2.text[:1000]}")
    except Exception as e:
        results.append(f"Lobby raw FAILED: {e}")

    return jsonify({"output": "\n".join(results)})


def _safe_int(val):
    try: return int(val)
    except: return 999


def _build_races_from_tab(tab_bundle):
    """Build race list from TAB data, including scratched/vacant runners."""
    odds = tab_bundle.get("odds", {})
    venue_names = tab_bundle.get("venue_names", {})
    race_meta = tab_bundle.get("race_meta", {})

    venues = {}
    for (vm, rnum), entry in odds.items():
        if vm not in venues:
            venues[vm] = {"name": venue_names.get(vm, vm), "races": {}}

        runners = []
        seen = set()
        # Add valid runners from all sources
        for src in ("tab_runners",):
            for r in entry.get(src, []):
                if r["number"] not in seen:
                    seen.add(r["number"])
                    runners.append({
                        "name": r["name"],
                        "number": r["number"],
                        "barrier": r["number"],
                        "lads_win": None,
                        "status": "valid",
                    })
        # Add scratched/vacant runners
        for s in entry.get("scratched", []):
            if s["number"] not in seen:
                seen.add(s["number"])
                runners.append({
                    "name": s["name"],
                    "number": s["number"],
                    "barrier": s["number"],
                    "lads_win": None,
                    "status": s.get("status", "nr"),
                })

        meta = race_meta.get((vm, rnum), {})
        start_time = meta.get("start_time", "")
        tab_status = meta.get("status", "Open").lower()
        if tab_status in ("final", "paying", "resulted", "interim"):
            race_status = "finished"
        elif tab_status in ("abandoned", "deleted"):
            race_status = "abandoned"
        else:
            race_status = "open"

        venues[vm]["races"][rnum] = {
            "runners": runners,
            "start_time": start_time,
            "race_status": race_status,
        }

    races = []
    for vm, info in venues.items():
        slug = info["name"].lower().replace(" ", "-").replace("'", "")
        for rnum in sorted(info["races"].keys()):
            rd = info["races"][rnum]
            races.append({
                "track": info["name"],
                "race_number": rnum,
                "start_time": rd["start_time"],
                "distance": "",
                "runners": rd["runners"],
                "venue_slug": slug,
                "race_id": f"{vm}-{rnum}",
                "race_status": rd["race_status"],
            })
    return races

def _normalize_name(name: str) -> str:
    return re.sub(r'\s*\(RES\)\s*', '', name, flags=re.IGNORECASE).strip().lower()


def _build_races_from_thedogs(dogs_data, tab_bundle):
    """Build race list from TheDogs schedule + TAB for race status.
    TheDogs provides complete runner fields; TAB provides status (open/finished/abandoned)."""
    race_meta = tab_bundle.get("race_meta", {})
    venue_names = tab_bundle.get("venue_names", {})

    # Build TAB status + time lookup: track_lower → {rnum → {status, start_time}}
    tab_lookup = {}
    for (vm, rnum), meta in race_meta.items():
        track_name = venue_names.get(vm, vm).strip().lower()
        tab_lookup.setdefault(track_name, {})[rnum] = {
            "status": meta.get("status", "Open"),
            "start_time": meta.get("start_time", ""),
        }

    races = []
    for meeting in dogs_data.get("meetings", []):
        track = meeting.get("track", "")
        slug = meeting.get("slug", "")
        track_lower = track.strip().lower()

        # Find matching TAB track
        tab_track = tab_lookup.get(track_lower, {})
        if not tab_track:
            for tk, data in tab_lookup.items():
                tk_slug = tk.replace(" ", "-").replace("'", "")
                if slug == tk_slug or slug.startswith(tk_slug) or tk_slug.startswith(slug):
                    tab_track = data
                    break

        for race_data in meeting.get("races", []):
            rnum = race_data.get("race_number", 0)
            dogs_time = race_data.get("start_time", "")

            # Prefer TAB time (authoritative), fall back to TheDogs
            tab_race = tab_track.get(rnum, {})
            start_time = tab_race.get("start_time") or dogs_time

            # Race status from TAB
            tab_st = tab_race.get("status", "Open").lower()
            if tab_st in ("final", "paying", "resulted", "interim"):
                race_status = "finished"
            elif tab_st in ("abandoned", "deleted"):
                race_status = "abandoned"
            else:
                race_status = "open"

            runners = []
            for r in race_data.get("runners", []):
                runners.append({
                    "name": r.get("name", ""),
                    "number": r.get("box", ""),
                    "barrier": r.get("box", ""),
                    "lads_win": None,
                    "status": r.get("status", "valid"),
                })

            races.append({
                "track": track,
                "race_number": rnum,
                "start_time": start_time,
                "distance": "",
                "runners": runners,
                "venue_slug": slug,
                "race_id": f"{slug}-{rnum}",
                "race_status": race_status,
            })

    return races


def _merge_lads_scratchings(tab_races, lads_races):
    """Merge LADS API scratching/VACANT/N/R status into TAB-built race list.
    Also flags runners missing from LADS as reserves.
    """
    if not lads_races:
        return

    # Build lookups per race
    lads_scratched = {}    # (track, rnum) → set of scratched runner numbers
    lads_all_nums = {}     # (track, rnum) → set of all runner numbers (when detail succeeded)

    for lr in lads_races:
        track = lr.get("track", "").strip().lower()
        rnum = lr.get("race_number")
        runners = lr.get("runners", [])
        if not runners:
            continue
        scratched_nums = set()
        all_nums = set()
        for runner in runners:
            num = str(runner.get("number", ""))
            all_nums.add(num)
            status = runner.get("status", "valid")
            if status in ("nr", "vacant"):
                scratched_nums.add(num)
        if scratched_nums:
            lads_scratched[(track, rnum)] = scratched_nums
        lads_all_nums[(track, rnum)] = all_nums

    for race in tab_races:
        if race.get("race_status", "open") != "open":
            continue
        track = race.get("track", "").strip().lower()
        rnum = race.get("race_number")
        scratched = lads_scratched.get((track, rnum), set())
        lads_nums = lads_all_nums.get((track, rnum))

        for runner in race.get("runners", []):
            num = str(runner.get("number", ""))
            # If LADS explicitly says scratched, mark as N/R (overrides TAB price)
            if num in scratched and runner.get("status") == "valid":
                runner["status"] = "nr"
                continue
            # If LADS has runners for this race but this runner is missing → flag as reserve
            if lads_nums and num not in lads_nums and runner.get("status") == "valid":
                runner["_note"] = "Reserve (not in LADS)"


def _convert_race_time(iso_str: str) -> dict:
    if not iso_str:
        return {"uk_time": "—", "ph_time": "—"}
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return {
            "uk_time": dt.astimezone(TZ_UK).strftime("%H:%M"),
            "ph_time": dt.astimezone(TZ_PH).strftime("%H:%M"),
        }
    except:
        return {"uk_time": "—", "ph_time": "—"}

def _build_runner_odds(runner, tab_odds_map, race_status="open", track="", rnum=0):
    """Build odds from TAB API. Use frozen prices for finished races."""
    result = {
        "tab_win": None,
    }
    if tab_odds_map:
        name = _normalize_name(runner.get("name", ""))
        num = str(runner.get("number", ""))
        odds = tab_odds_map.get(name)
        if not odds and num:
            odds = tab_odds_map.get(num)
        if odds:
            result["tab_win"] = odds.get("tab_win")

    # For finished races, use frozen prices if current ones are missing
    if race_status == "finished":
        fkey = (track, rnum, str(runner.get("number", "")))
        frozen = _frozen_prices.get(fkey, {})
        if result["tab_win"] is None and frozen.get("tab_win"):
            result["tab_win"] = frozen["tab_win"]
    # Store prices for future freezing (only if we have them)
    if result["tab_win"]:
        fkey = (track, rnum, str(runner.get("number", "")))
        _frozen_prices[fkey] = {k: v for k, v in result.items() if v is not None}

    return result


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)

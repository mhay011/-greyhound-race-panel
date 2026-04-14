"""
Runner: fetch → normalise → analyse → summary.

Usage:
    python -m lads_au_checker.run_check              # today
    python -m lads_au_checker.run_check 2026-04-10   # specific date
    python -m lads_au_checker.run_check 2026-04-10 --threshold 110
"""
import sys
import json
import logging
from datetime import date

from .adapters.lads_au_greyhounds import fetch_greyhound_races
from .normalise import normalise_lads_au_response
from .analysis import analyse_races

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def check(race_date: str | None = None, threshold: float | None = None) -> dict:
    """
    End-to-end check: are LADS AU Greyhounds prices available?

    Returns a JSON-serialisable summary dict.
    """
    if race_date is None:
        race_date = date.today().isoformat()

    errors: list[str] = []

    # 1. Fetch
    try:
        raw_races = fetch_greyhound_races(race_date)
    except Exception as exc:
        errors.append(f"Fetch failed: {exc}")
        raw_races = []

    # 2. Normalise
    try:
        normalised = normalise_lads_au_response(raw_races)
    except Exception as exc:
        errors.append(f"Normalise failed: {exc}")
        normalised = []

    # 3. Analyse
    try:
        analysed = analyse_races(normalised, threshold=threshold)
    except Exception as exc:
        errors.append(f"Analysis failed: {exc}")
        analysed = []

    # 4. Summary counts
    races_total = len(analysed)
    races_with_prices = sum(
        1 for r in analysed if not r.get("no_prices") and len(r.get("runners", [])) > 0
    )

    return {
        "source": "LADS_AU",
        "sport": "GREYHOUNDS",
        "date": race_date,
        "races_total": races_total,
        "races_with_prices": races_with_prices,
        "races": analysed,
        "errors": errors,
    }


def main():
    race_date = None
    threshold = None
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--threshold" and i + 1 < len(args):
            threshold = float(args[i + 1])
        elif not arg.startswith("--") and race_date is None:
            race_date = arg

    result = check(race_date=race_date, threshold=threshold)
    print(json.dumps(result, indent=2))

    # Quick human-readable summary
    print(f"\n--- LADS AU Greyhounds: {result['date']} ---")
    print(f"Races total:       {result['races_total']}")
    print(f"Races with prices: {result['races_with_prices']}")
    if result["errors"]:
        print(f"Errors:            {len(result['errors'])}")
        for e in result["errors"]:
            print(f"  • {e}")
    for r in result["races"]:
        tag = "WIN-ONLY" if r["win_only"] else "EW"
        prices = "NO PRICES" if r["no_prices"] else f"Win%={r['total_win_percent']}  EW%={r['total_ew_percent']}"
        print(f"  {r['course']} R{r['race_no']}  [{tag}]  {prices}")


if __name__ == "__main__":
    main()

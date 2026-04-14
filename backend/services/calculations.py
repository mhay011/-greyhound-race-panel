"""
Calculation services for implied probabilities and Each Way evaluation.

Price sources: LADS (Ladbrokes One API), TAB (QLD)
EW % = 100 / [ ((Win Price − 1) × EW Fraction) + 1 ]
Bad EW priority: LADS → TAB (first available)
Bad EW if: Total EW % ≤ (Places Paid × 100) + 10
"""
import logging
log = logging.getLogger(__name__)


def get_ew_terms(num_runners: int, is_handicap: bool = False) -> tuple[float, int]:
    if is_handicap:
        if num_runners >= 16: return 0.25, 4
        elif num_runners >= 12: return 0.25, 3
    if num_runners >= 8: return 0.20, 3
    elif num_runners >= 5: return 0.25, 2
    return 0, 0


def calc_place_odds(win_odds, ew_fraction):
    if win_odds is None or win_odds <= 0 or ew_fraction <= 0: return None
    return ((win_odds - 1) * ew_fraction) + 1


def calc_implied_prob(odds):
    if odds is None or odds <= 0: return None
    return round(100 / odds, 2)


def calculate_runner_probabilities(runner_odds: dict, ew_fraction: float = 0.20) -> dict:
    result = {}
    for prefix, win_key in [("lads", "lads_win"), ("tab", "tab_win")]:
        win = runner_odds.get(win_key)
        result[f"{prefix}_win_pct"] = calc_implied_prob(win)
        result[f"{prefix}_ew_pct"] = calc_implied_prob(calc_place_odds(win, ew_fraction))
    return result


def _sum_pcts(runners, key):
    total, has = 0.0, False
    for r in runners:
        v = r.get(key)
        if v is not None: total += v; has = True
    return round(total, 2) if has else None


def calculate_sportsbook_totals(runners):
    return {
        "lads_total_win_pct": _sum_pcts(runners, "lads_win_pct"),
        "lads_total_ew_pct": _sum_pcts(runners, "lads_ew_pct"),
        "tab_total_win_pct": _sum_pcts(runners, "tab_win_pct"),
        "tab_total_ew_pct": _sum_pcts(runners, "tab_ew_pct"),
    }


def evaluate_each_way_value(runners, num_runners, is_handicap=False):
    ew_fraction, places_paid = get_ew_terms(num_runners, is_handicap)
    totals = calculate_sportsbook_totals(runners)

    if places_paid == 0:
        return {"places_paid": 0, "ew_fraction": 0, "bad_each_way": False,
                "reason": "Win Only", "total_ew_pct": 0, "threshold": 0, "totals": totals}

    threshold = places_paid * 100

    # Priority: LADS → TAB
    ew_pct_sources = [
        ("LADS", totals["lads_total_ew_pct"]),
        ("TAB", totals["tab_total_ew_pct"]),
    ]

    has_prices = any(v is not None for _, v in ew_pct_sources)
    if not has_prices:
        return {"places_paid": places_paid, "ew_fraction": ew_fraction, "bad_each_way": False,
                "reason": "No prices available.", "total_ew_pct": 0, "threshold": threshold, "totals": totals}

    eval_source = None
    total_ew_pct = 0
    for name, val in ew_pct_sources:
        if val is not None:
            eval_source = name
            total_ew_pct = val
            break

    bad_ew_threshold = threshold
    nearly_threshold = threshold + 20
    is_bad = total_ew_pct <= bad_ew_threshold
    is_nearly_bad = not is_bad and total_ew_pct <= nearly_threshold
    reason = ""
    if is_bad:
        reason = f"Total EW {total_ew_pct}% ≤ {bad_ew_threshold}% (via {eval_source})"
    elif is_nearly_bad:
        reason = f"Nearly Bad EW: {total_ew_pct}% ≤ {nearly_threshold}% (via {eval_source})"

    return {
        "places_paid": places_paid, "ew_fraction": ew_fraction,
        "bad_each_way": is_bad, "nearly_bad_ew": is_nearly_bad,
        "reason": reason,
        "total_ew_pct": total_ew_pct, "threshold": threshold,
        "eval_source": eval_source, "totals": totals,
    }

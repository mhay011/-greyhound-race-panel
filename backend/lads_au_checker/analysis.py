"""
Deterministic analysis functions for LADS AU Greyhounds.

calc_win_percent / calc_ew_percent / analyse_race / analyse_races

These are pure functions — no I/O, no side effects.
"""


def calc_win_percent(decimal_price: float | None) -> float | None:
    """Implied win probability: 100 / decimal_price."""
    if decimal_price is None or decimal_price <= 0:
        return None
    return round(100.0 / decimal_price, 2)


def calc_ew_percent(
    decimal_price: float | None,
    ew_factor_num: int = 1,
    ew_factor_den: int = 5,
) -> float | None:
    """
    Implied EW probability.
    EW odds = ((win_price - 1) * (ew_factor_num / ew_factor_den)) + 1
    EW % = 100 / ew_odds
    """
    if decimal_price is None or decimal_price <= 0:
        return None
    if ew_factor_den == 0:
        return None
    fraction = ew_factor_num / ew_factor_den
    ew_odds = ((decimal_price - 1) * fraction) + 1
    if ew_odds <= 0:
        return None
    return round(100.0 / ew_odds, 2)


def analyse_race(race_data: dict, threshold: float | None = None) -> dict:
    """
    Analyse a single normalised race.

    Returns dict with:
        course, race_no, time, win_only, no_prices,
        runners (with win_pct, ew_pct),
        total_win_percent, total_ew_percent,
        threshold_exceeded (bool)
    """
    ew_terms = race_data.get("ew_terms")
    win_only = ew_terms is None
    runners_out = []
    total_win = 0.0
    total_ew = 0.0
    has_any_price = False

    for rp in race_data.get("runners_prices", []):
        dec = rp.get("decimal_price")
        win_pct = calc_win_percent(dec)
        ew_pct = None
        if not win_only and ew_terms:
            ew_pct = calc_ew_percent(
                dec,
                ew_terms.get("ew_factor_num", 1),
                ew_terms.get("ew_factor_den", 5),
            )

        if win_pct is not None:
            has_any_price = True
            total_win += win_pct
        if ew_pct is not None:
            total_ew += ew_pct

        runners_out.append({
            "number": rp.get("number", ""),
            "name": rp.get("name", ""),
            "decimal_price": dec,
            "win_pct": win_pct,
            "ew_pct": ew_pct,
        })

    total_win = round(total_win, 2)
    total_ew = round(total_ew, 2)

    # Threshold check (default: 100%)
    effective_threshold = threshold if threshold is not None else 100.0
    threshold_exceeded = total_win > effective_threshold if has_any_price else False

    return {
        "course": race_data.get("course", ""),
        "race_no": race_data.get("race_no", 0),
        "time": race_data.get("time", ""),
        "win_only": win_only,
        "no_prices": not has_any_price,
        "runners": runners_out,
        "total_win_percent": total_win,
        "total_ew_percent": total_ew,
        "threshold_exceeded": threshold_exceeded,
    }


def analyse_races(
    races: list[dict],
    threshold: float | None = None,
) -> list[dict]:
    """Analyse a list of normalised race_data dicts."""
    return [analyse_race(r, threshold=threshold) for r in races]

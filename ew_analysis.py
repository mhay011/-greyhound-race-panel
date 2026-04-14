"""
EW Analysis — Pure calculation functions for Win % and EW % analysis.
No I/O dependencies. All calculations are deterministic.
"""

DEFAULT_BAD_EW_THRESHOLD = 150.0  # Total EW % above this = bad EW race


def calc_win_percent(num_price, den_price):
    """
    Calculate implied win probability.
    Win % = 100 / ((num_price / den_price) + 1)
    Returns None if den_price is 0.
    """
    if not den_price or den_price == 0:
        return None
    price = num_price / den_price
    return round(100 / (price + 1), 2)


def calc_ew_percent(decimal_price, ew_factor_num, ew_factor_den):
    """
    Calculate implied EW probability using decimal price.
    EW % = 100 / (((decimalPrice - 1) / (ew_factor_den / ew_factor_num)) + 1)
    
    For 1/5 EW: divisor = 5/1 = 5
    e.g. decimal 34.00, 1/5 EW: 100 / (((34-1)/5) + 1) = 100 / 7.6 = 13.16%
    
    Returns 100.00 when denominator equals zero.
    Returns None if inputs are invalid.
    """
    if not ew_factor_num or ew_factor_num == 0:
        return None
    if not ew_factor_den or ew_factor_den == 0:
        return None
    if decimal_price is None or decimal_price <= 0:
        return None
    
    # EW divisor: e.g. 1/5 means divide by 5 (den/num)
    ew_divisor = ew_factor_den / ew_factor_num
    
    ew_price = (decimal_price - 1) / ew_divisor
    denominator = ew_price + 1
    
    if denominator == 0:
        return 100.00
    
    return round(100 / denominator, 2)


def analyse_race(race_data):
    """
    Analyse a single race for EW value.
    
    Args:
        race_data: dict with keys: time, course, ew_terms, runners_prices
        
    Returns:
        Enriched race dict with per-runner Win%/EW% and totals, or None if no EW terms.
    """
    ew_terms = race_data.get("ew_terms")
    runners_prices = race_data.get("runners_prices", [])
    
    if not ew_terms:
        # Win Only race — no EW terms available
        return {
            "time": race_data.get("time", ""),
            "course": race_data.get("course", ""),
            "race_no": race_data.get("race_no", 0),
            "ew_terms": None,
            "runners": [],
            "total_win_percent": 0.0,
            "total_ew_percent": 0.0,
            "win_only": True,
            "no_prices": False,
        }
    
    ew_factor_num = ew_terms.get("ew_factor_num", 0)
    ew_factor_den = ew_terms.get("ew_factor_den", 0)
    ew_places = ew_terms.get("ew_places", 0)
    
    if not ew_factor_num or not ew_factor_den:
        return {
            "time": race_data.get("time", ""),
            "course": race_data.get("course", ""),
            "race_no": race_data.get("race_no", 0),
            "ew_terms": None,
            "runners": [],
            "total_win_percent": 0.0,
            "total_ew_percent": 0.0,
            "win_only": True,
            "no_prices": False,
        }
    
    runners_prices = race_data.get("runners_prices", [])
    analysed_runners = []
    
    for runner in runners_prices:
        num_p = runner.get("num_price")
        den_p = runner.get("den_price")
        dec_p = runner.get("decimal_price")
        
        if num_p is None or den_p is None or den_p == 0:
            continue
        if dec_p is None or dec_p <= 0:
            # Fallback: compute decimal from fractional
            dec_p = (num_p / den_p) + 1
        
        win_pct = calc_win_percent(num_p, den_p)
        ew_pct = calc_ew_percent(dec_p, ew_factor_num, ew_factor_den)
        
        if win_pct is None or ew_pct is None:
            continue
        
        analysed_runners.append({
            "number": runner.get("number", "?"),
            "name": runner.get("name", "Unknown"),
            "num_price": num_p,
            "den_price": den_p,
            "win_percent": win_pct,
            "ew_percent": ew_pct,
        })
    
    if not analysed_runners:
        # Still show the race even with no priced runners
        return {
            "time": race_data.get("time", ""),
            "course": race_data.get("course", ""),
            "race_no": race_data.get("race_no", 0),
            "ew_terms": {
                "ew_factor_num": ew_factor_num,
                "ew_factor_den": ew_factor_den,
                "ew_places": ew_places,
            },
            "runners": [],
            "total_win_percent": 0.0,
            "total_ew_percent": 0.0,
            "no_prices": True,
        }
    
    total_win = round(sum(r["win_percent"] for r in analysed_runners), 2)
    total_ew = round(sum(r["ew_percent"] for r in analysed_runners), 2)
    
    return {
        "time": race_data.get("time", ""),
        "course": race_data.get("course", ""),
        "race_no": race_data.get("race_no", 0),
        "ew_terms": {
            "ew_factor_num": ew_factor_num,
            "ew_factor_den": ew_factor_den,
            "ew_places": ew_places,
        },
        "runners": analysed_runners,
        "total_win_percent": total_win,
        "total_ew_percent": total_ew,
    }


def analyse_races(races, threshold=None):
    """
    Analyse multiple races and flag bad EW races.
    
    Threshold is based on EW places: places * 100%
    e.g. Top 3 places = 300%, Top 2 = 200%, Top 4 = 400%
    
    A race is BAD EW when total_ew_percent <= threshold.
    
    Args:
        races: list of race dicts from fetch_ew_data_for_date()
        threshold: override threshold (if None, calculated from ew_places)
        
    Returns:
        List of analysed race dicts with is_bad_ew flag and threshold.
    """
    results = []
    for race in races:
        analysed = analyse_race(race)
        if analysed is None:
            continue
        # Threshold = ew_places * 100
        ew_places = analysed["ew_terms"]["ew_places"]
        race_threshold = threshold if threshold is not None else ew_places * 100.0
        analysed["threshold"] = race_threshold
        analysed["is_bad_ew"] = analysed["total_ew_percent"] <= race_threshold
        results.append(analysed)
    
    return results

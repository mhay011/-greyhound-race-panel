"""
Normalise raw Ladbrokes AU adapter output into the race_data structure
expected by the analysis layer.

No I/O here — pure data transformation.
"""


def normalise_lads_au_response(raw_races: list[dict]) -> list[dict]:
    """
    Convert adapter output → list of normalised race_data dicts.

    Robust: missing fields produce partial records rather than crashes.
    """
    normalised = []
    for raw in raw_races:
        runners_prices = []
        for r in raw.get("runners", []):
            if r.get("status") not in ("valid", None):
                continue
            runners_prices.append({
                "number": r.get("number", ""),
                "name": r.get("name", "Unknown"),
                "num_price": r.get("num_price"),
                "den_price": r.get("den_price"),
                "decimal_price": r.get("decimal_price"),
            })

        normalised.append({
            "time": raw.get("start_time", ""),
            "course": raw.get("track", "Unknown"),
            "race_no": raw.get("race_number", 0),
            "ew_terms": raw.get("ew_terms"),  # None for win-only
            "runners_prices": runners_prices,
        })
    return normalised

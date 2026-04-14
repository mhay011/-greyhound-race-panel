"""
Test fixtures: raw adapter output simulating three scenarios.
"""

# 1. Race with full prices + EW terms
RACE_WITH_PRICES = {
    "track": "Sandown Park",
    "race_number": 1,
    "start_time": "2026-04-10T10:00:00+10:00",
    "event_id": "EVT001",
    "ew_terms": {"ew_factor_num": 1, "ew_factor_den": 5, "ew_places": 3},
    "runners": [
        {"name": "Speedy Dog", "number": "1", "status": "valid",
         "num_price": 3.0, "den_price": 1.0, "decimal_price": 4.0},
        {"name": "Quick Paws", "number": "2", "status": "valid",
         "num_price": 5.0, "den_price": 2.0, "decimal_price": 3.5},
        {"name": "Flash Runner", "number": "3", "status": "valid",
         "num_price": 9.0, "den_price": 1.0, "decimal_price": 10.0},
        {"name": "Bolt", "number": "4", "status": "valid",
         "num_price": 1.0, "den_price": 1.0, "decimal_price": 2.0},
        {"name": "Scratched One", "number": "5", "status": "nr",
         "num_price": None, "den_price": None, "decimal_price": None},
    ],
}

# 2. Race with NO prices (future race, no market yet)
RACE_NO_PRICES = {
    "track": "The Meadows",
    "race_number": 3,
    "start_time": "2026-04-10T14:00:00+10:00",
    "event_id": "EVT002",
    "ew_terms": None,
    "runners": [
        {"name": "Runner A", "number": "1", "status": "valid",
         "num_price": None, "den_price": None, "decimal_price": None},
        {"name": "Runner B", "number": "2", "status": "valid",
         "num_price": None, "den_price": None, "decimal_price": None},
    ],
}

# 3. Win-only race (no EW terms — typical for greyhounds)
RACE_WIN_ONLY = {
    "track": "Wentworth Park",
    "race_number": 5,
    "start_time": "2026-04-10T19:30:00+10:00",
    "event_id": "EVT003",
    "ew_terms": None,
    "runners": [
        {"name": "Night Flyer", "number": "1", "status": "valid",
         "num_price": 2.0, "den_price": 1.0, "decimal_price": 3.0},
        {"name": "Dark Star", "number": "2", "status": "valid",
         "num_price": 7.0, "den_price": 2.0, "decimal_price": 4.5},
        {"name": "Shadow", "number": "3", "status": "valid",
         "num_price": 1.0, "den_price": 4.0, "decimal_price": 1.25},
    ],
}

ALL_RAW_RACES = [RACE_WITH_PRICES, RACE_NO_PRICES, RACE_WIN_ONLY]

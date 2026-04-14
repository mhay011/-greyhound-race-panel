"""
Tests for normalisation and analysis — deterministic, no network.
"""
import unittest
from unittest.mock import patch

from ..normalise import normalise_lads_au_response
from ..analysis import calc_win_percent, calc_ew_percent, analyse_race, analyse_races
from ..run_check import check
from .fixtures import ALL_RAW_RACES, RACE_WITH_PRICES, RACE_NO_PRICES, RACE_WIN_ONLY


class TestCalcFunctions(unittest.TestCase):
    def test_win_percent_basic(self):
        self.assertAlmostEqual(calc_win_percent(4.0), 25.0)
        self.assertAlmostEqual(calc_win_percent(2.0), 50.0)

    def test_win_percent_none(self):
        self.assertIsNone(calc_win_percent(None))
        self.assertIsNone(calc_win_percent(0))
        self.assertIsNone(calc_win_percent(-1))

    def test_ew_percent_basic(self):
        # decimal 4.0, 1/5 fraction → EW odds = (3*0.2)+1 = 1.6 → 62.5%
        self.assertAlmostEqual(calc_ew_percent(4.0, 1, 5), 62.5)

    def test_ew_percent_none_inputs(self):
        self.assertIsNone(calc_ew_percent(None))
        self.assertIsNone(calc_ew_percent(4.0, 1, 0))


class TestNormalise(unittest.TestCase):
    def test_normalise_filters_scratched(self):
        result = normalise_lads_au_response([RACE_WITH_PRICES])
        self.assertEqual(len(result), 1)
        # Scratched runner (status=nr) should be excluded
        self.assertEqual(len(result[0]["runners_prices"]), 4)

    def test_normalise_preserves_ew_terms(self):
        result = normalise_lads_au_response([RACE_WITH_PRICES])
        self.assertIsNotNone(result[0]["ew_terms"])
        self.assertEqual(result[0]["ew_terms"]["ew_factor_den"], 5)

    def test_normalise_win_only(self):
        result = normalise_lads_au_response([RACE_WIN_ONLY])
        self.assertIsNone(result[0]["ew_terms"])

    def test_normalise_no_prices(self):
        result = normalise_lads_au_response([RACE_NO_PRICES])
        # Runners still present, just no decimal_price
        self.assertEqual(len(result[0]["runners_prices"]), 2)
        self.assertIsNone(result[0]["runners_prices"][0]["decimal_price"])


class TestAnalyseRace(unittest.TestCase):
    def setUp(self):
        self.normalised = normalise_lads_au_response(ALL_RAW_RACES)

    def test_race_with_prices(self):
        r = analyse_race(self.normalised[0])
        self.assertFalse(r["no_prices"])
        self.assertFalse(r["win_only"])
        self.assertGreater(r["total_win_percent"], 0)
        self.assertGreater(r["total_ew_percent"], 0)
        self.assertEqual(r["course"], "Sandown Park")

    def test_race_no_prices(self):
        r = analyse_race(self.normalised[1])
        self.assertTrue(r["no_prices"])
        self.assertEqual(r["total_win_percent"], 0)

    def test_race_win_only(self):
        r = analyse_race(self.normalised[2])
        self.assertTrue(r["win_only"])
        self.assertFalse(r["no_prices"])
        self.assertGreater(r["total_win_percent"], 0)
        # EW should be 0 for win-only
        self.assertEqual(r["total_ew_percent"], 0)

    def test_analyse_races_count(self):
        results = analyse_races(self.normalised)
        self.assertEqual(len(results), 3)

    def test_threshold_exceeded(self):
        r = analyse_race(self.normalised[0], threshold=50.0)
        # total_win_percent for fixture is ~25+28.57+10+50 = 113.57 → exceeds 50
        self.assertTrue(r["threshold_exceeded"])


class TestCheckIntegration(unittest.TestCase):
    @patch("lads_au_checker.run_check.fetch_greyhound_races")
    def test_check_with_mocked_adapter(self, mock_fetch):
        mock_fetch.return_value = ALL_RAW_RACES
        result = check(race_date="2026-04-10")
        self.assertEqual(result["source"], "LADS_AU")
        self.assertEqual(result["sport"], "GREYHOUNDS")
        self.assertEqual(result["races_total"], 3)
        # 2 races have prices (RACE_WITH_PRICES and RACE_WIN_ONLY)
        self.assertEqual(result["races_with_prices"], 2)
        self.assertEqual(len(result["errors"]), 0)


if __name__ == "__main__":
    unittest.main()

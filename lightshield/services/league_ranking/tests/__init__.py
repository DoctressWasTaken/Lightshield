import unittest

from lightshield.services.league_ranking import Handler


class TestLeagueRanking(unittest.TestCase):
    def test_init(self):
        Handler()

        self.assertTrue(True)

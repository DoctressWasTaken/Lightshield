"""Database models"""
from django.db import models

# Create your models here.


class Summoner(models.Model):
    """Summoner database model.

    Contains the IDs relevant for each user as well as their current
    soloQ ranking.
    """
    # IDs
    summonerId = models.CharField(max_length=63)
    accountId = models.CharField(max_length=56)
    puuid = models.CharField(max_length=78)

    # Stats
    summonerName = models.CharField(max_length=16)
    wins = models.IntegerField(default=0)
    losses = models.IntegerField(default=0)

    tier = models.IntegerField(default=0)
    ranking = models.IntegerField(default=0)
    series = models.CharField(max_length=4, null=True)

    def set_ranking(self,
                    tier: str,
                    rank: str,
                    leaguePoints: int,
                    miniSeries: dict = None):
        """Set ranking for the player.

        Calculates a linear number based on total LP for each player.
        In addition saves current miniseries as string (4 characters)
        In addition saves the current tier, to differentiate between Master,
        Grandmaster and Challenger.
        """
        map_tiers = {
            'IRON': 0,
            'BRONZE': 4,
            'SILVER': 8,
            'GOLD': 12,
            'PLATINUM': 16,
            'DIAMOND': 20,
            'MASTER': 24,
            'GRANDMASTER': 24,
            'CHALLENGER': 24}
        map_tiers_numeric = {
            'IRON': 0,
            'BRONZE': 1,
            'SILVER': 2,
            'GOLD': 3,
            'PLATINUM': 4,
            'DIAMOND': 5,
            'MASTER': 6,
            'GRANDMASTER': 7,
            'CHALLENGER': 8}
        map_rank = {
            'IV': 0,
            'III': 1,
            'II': 2,
            'I': 3}
        ranking = map_tiers[tier]
        ranking += map_rank[rank]
        ranking += leaguePoints
        self.ranking = ranking
        self.tier = map_tiers_numeric[tier]
        if miniSeries:
            self.series = miniSeries['progress'][:-1]


from django.db import models

# Create your models here.

map_tiers = [
    ('IRON', 0),
    ('BRONZE', 400),
    ('SILVER', 800),
    ('GOLD', 1200),
    ('PLATINUM', 1600),
    ('DIAMOND', 2000)
]
map_divisions = [
    ('IV', 0),
    ('III', 100),
    ('II', 200),
    ('I', 300)
]


class Player(models.Model):

    server = models.CharField(max_length=4)
    summoner_name = models.TextField(null=True)  # Source: League API

    # IDs
    summoner_id = models.TextField()  # Source: League API
    account_id = models.TextField(null=True)  # Source: Summoner API
    puuid = models.TextField(null=True)  # Source: Summoner API

    requested_ids = models.DateTimeField(null=True)

    # Ranked data (Source: League API)
    ranking_solo = models.IntegerField(null=True)
    ranking_flex = models.IntegerField(null=True)

    series_solo = models.CharField(max_length=4, null=True)
    series_flex = models.CharField(max_length=4,  null=True)

    wins_solo = models.IntegerField(null=True)
    losses_solo = models.IntegerField(null=True)

    wins_flex = models.IntegerField(null=True)
    losses_flex = models.IntegerField(null=True)

    def update(self, data):

        self.summoner_name = data['summonerName']
        points = 0
        for tier in map_tiers:
            if  tier[0] == data['tier']:
                points += tier[1]
                break
        for div in map_divisions:
            if div[0] == data['rank']:
                points += div[1]
                break
        points += data['leaguePoints']
        series = None
        if 'miniSeries' in data:
            series = data['miniSeries']['progress'][:4]
        if data['queueType'] == 'RANKED_SOLO_5x5':
            self.ranking_solo = points
            self.wins_solo = data['wins']
            self.losses_solo = data['losses']
            self.series_solo = series
        elif data['queueType'] == 'RANKED_FLEX_SR':
            self.ranking_flex = points
            self.wins_flex = data['wins']
            self.losses_flex = data['losses']
            self.series_flex = series

class Page(models.Model):
    """Page containing players.

    Has a specific tier, division, queue and page number.
    The last ones are marked.
    If the last page is called and contains data a new page is added until the last page is empty.
    """
    server = models.TextField()
    tier = models.TextField()
    division = models.TextField()
    queue = models.TextField()
    page = models.IntegerField()
    last = models.BooleanField(default=True)
    requested = models.BooleanField(default=False)

    last_updated = models.DateTimeField(
        auto_now=True)

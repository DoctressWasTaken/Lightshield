"""Manage which rank is to be crawled next."""
from datetime import datetime, timedelta

from lightshield import settings

tiers = [
    "IRON",
    "BRONZE",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "DIAMOND",
    "MASTER",
    "GRANDMASTER",
    "CHALLENGER",
]

divisions = ["IV", "III", "II", "I"]


class RankManager:
    """Ordering and Management of ranking updates."""

    def __init__(self, logging):
        """Initiate logging."""
        self.logging = logging
        self.ranks = None

    async def init(self):
        """Open or create the ranking_cooldown tracking sheet."""
        now = datetime.timestamp(
            datetime.now() - timedelta(hours=settings.LEAGUE_UPDATE)
        )
        self.ranks = []
        for tier in tiers:
            if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                self.ranks.append([tier, "I", now])
                continue
            for division in divisions:
                self.ranks.append([tier, division, now])

    async def get_next(self):
        """Return the next tier/division combination to be called."""
        oldest_timestamp = None
        oldest_key = None
        for entry in self.ranks:
            if not oldest_timestamp or entry[2] < oldest_timestamp:
                oldest_key = entry[0:2]
                oldest_timestamp = entry[2]
        return oldest_key

    async def update(self, key):
        """Update the stats on a rank that is done pulling."""
        self.logging.info("Done with %s.", key)
        now = datetime.timestamp(datetime.now())
        for index, entry in enumerate(self.ranks):
            if entry[0] == key[0] and entry[1] == key[1]:
                self.ranks[index][2] = now
                return

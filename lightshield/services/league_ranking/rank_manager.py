"""Manage which rank is to be crawled next."""
from datetime import datetime, timedelta
import asyncio


class RankManager:
    """Ordering and Management of ranking updates."""

    def __init__(self, config, logging, handler):
        """Initiate logging."""
        service = config['services']['league_ranking']
        self.handler = handler
        self.logging = logging
        self.ranks = None
        self.tiers = service.get('tiers')
        self.divisions = config['statics']['division']
        self.cycle_length = service.get('cycle_min_length_hours')

    async def init(self):
        """Open or create the ranking_cooldown tracking sheet."""
        now = datetime.now()
        self.ranks = []
        for tier in self.tiers:
            if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                self.ranks.append([tier, "I", now])
                continue
            for division in self.divisions:
                self.ranks.append([tier, division, now])

    async def get_next(self):
        """Return the next tier/division combination to be called."""
        oldest_timestamp = None
        oldest_key = None
        for entry in self.ranks:
            if not oldest_timestamp or entry[2] < oldest_timestamp:
                oldest_key = entry[0:2]
                oldest_timestamp = entry[2]
        if oldest_timestamp > datetime.now():
            self.logging.info("Not ready yet, continuing at %s", oldest_timestamp)
        while oldest_timestamp > datetime.now() and not self.handler.is_shutdown:
            await asyncio.sleep(5)
        return oldest_key

    async def update(self, key):
        """Update the stats on a rank that is done pulling."""
        now = datetime.now() + timedelta(hours=self.cycle_length)
        for index, entry in enumerate(self.ranks):
            if entry[0] == key[0] and entry[1] == key[1]:
                self.ranks[index][2] = now
                return

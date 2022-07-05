"""Manage which rank is to be crawled next."""
import asyncio
from datetime import datetime, timedelta


class RankManager:
    """Ordering and Management of ranking updates."""

    tasks = None

    def __init__(self, config, logging, handler):
        """Initiate logging."""
        service = config.services.league_ranking
        self.handler = handler
        self.logging = logging
        self.ranks = service.ranks or config.statics.enums.ranks
        self.divisions = config.statics.enums.divisions
        self.cycle_length = service.cycle_length

    async def init(self):
        """Open or create the ranking_cooldown tracking sheet."""
        now = datetime.now()
        self.tasks = []
        for rank in self.ranks:
            if rank in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                self.tasks.append([rank, "I", now])
                continue
            for division in self.divisions:
                self.tasks.append([rank, division, now])

    async def get_next(self):
        """Return the next tier/division combination to be called."""
        oldest_timestamp = None
        oldest_key = None
        for entry in self.tasks:
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
        for index, entry in enumerate(self.tasks):
            if entry[0] == key[0] and entry[1] == key[1]:
                self.tasks[index][2] = now
                return

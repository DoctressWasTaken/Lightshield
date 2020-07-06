"""Manage which rank is to be crawled next."""
import json
import os
import asyncio
import logging
from datetime import datetime, timedelta

tiers = [
    "IRON",
    "BRONZE",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "DIAMOND",
    "MASTER",
    "GRANDMASTER",
    "CHALLENGER"]

divisions = [
    "IV",
    "III",
    "II",
    "I"]


class RankManager:
    """Ordering and Management of ranking updates."""

    def __init__(self):
        """Initiate logging."""
        self.logging = logging.getLogger("RankManager")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [RankManager] %(message)s'))
        self.logging.addHandler(handler)

        self.ranks = None
        self.update_interval = int(os.environ['UPDATE_INTERVAL'])

    async def init(self):
        """Open or create the ranking_cooldown tracking sheet."""
        try:
            self.ranks = json.loads(open("configs/ranking_cooldown.json", "r+").read())
            self.logging.info("Loaded data file.")
        except FileNotFoundError:
            self.logging.info("File not found. Recreating.")
            now = datetime.timestamp(datetime.now() - timedelta(hours=self.update_interval))
            self.ranks = []
            for tier in tiers:
                if tier in ['MASTER', 'GRANDMASTER', 'CHALLENGER']:
                    self.ranks.append([tier, 'I', now])
                    continue
                for division in divisions:
                    self.ranks.append([tier, division, now])
        await self.save_to_file()

    async def save_to_file(self):
        """Save the current stats to the tracking file."""
        with open("configs/ranking_cooldown.json", "w+") as datafile:
            datafile.write(json.dumps(self.ranks))

    async def get_next(self):
        """Return the next tier/division combination to be called."""
        oldest_timestamp = None
        oldest_key = None
        for entry in self.ranks:
            if not oldest_timestamp or entry[2] < oldest_timestamp:
                oldest_key = entry[0:2]
                oldest_timestamp = entry[2]
        if (total_seconds := (datetime.now() - datetime.fromtimestamp(
                oldest_timestamp)).total_seconds() - self.update_interval * 3600) > 0:
            self.logging.info("Waiting for %s seconds before starting next element.",
                              total_seconds)
            await asyncio.sleep(total_seconds)
        self.logging.info("Commencing on %s.", oldest_key)
        return oldest_key

    async def update(self, key):
        """Update the stats on a rank that is done pulling."""
        self.logging.info("Done with %s.", key)
        now = datetime.timestamp(datetime.now())
        for index, entry in enumerate(self.ranks):
            if entry[0] == key[0] and entry[1] == key[1]:
                self.ranks[index][2] = now
                await self.save_to_file()
                return

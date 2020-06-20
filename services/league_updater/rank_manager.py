"""Manage which rank is to be crawled next."""
import json
import logging
from datetime import datetime
import asyncio

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

    def __init__(self):
        self.logging = logging.getLogger("RankManager")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [RankManager] %(message)s'))
        self.logging.addHandler(ch)

    async def init(self):
        """Open or create the ranking_cooldown tracking sheet."""
        try:
            self.ranks = json.loads(open("ranking_cooldown.json", "r+").read())
            self.logging.info("Loaded data file.")
        except FileNotFoundError:
            self.logging.info("File not found. Recreating.")
            now = datetime.timestamp(datetime.now())
            self.ranks = []
            for tier in tiers:
                if tier in ['MASTER', 'GRANDMASTER', 'CHALLENGER']:
                    self.ranks.append([tier, 'I', now])
                    continue
                for division in divisions:
                    self.ranks.append([tier, division, now])
        await self.save_to_file()

    async def get_total(self):
        """Return the number of entries in the file.

        Allows to call each entry once.
        """
        return len(self.ranks)

    async def save_to_file(self):
        """Save the current stats to the tracking file."""
        with open("ranking_cooldown.json", "w+") as datafile:
            datafile.write(json.dumps(self.ranks))

    async def get_next(self):
        """Return the next tier/division combination to be called."""
        oldest_timestamp = None
        oldest_key = None
        for entry in self.ranks:
            if not oldest_timestamp or entry[2] < oldest_timestamp:
                oldest_key = entry[0:2]
                oldest_timestamp = entry[2]
        self.logging.info(f"Commencing on {oldest_key}.")
        return oldest_key

    async def update(self, key):
        """Update the stats on a rank that is done pulling."""
        self.logging.info(f"Done with {key}.")
        now = datetime.timestamp(datetime.now())
        for index, entry in enumerate(self.ranks):
            if entry[0] == key[0] and entry[1] == key[1]:
                self.ranks[index][2] = now
                await self.save_to_file()
                return

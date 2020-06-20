"""Manage which rank is to be crawled next."""
import json
import logging
from datetime import datetime

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
        """Open or create the ranking_cooldown tracking sheet."""
        self.logging = logging.getLogger("RankManager")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [RankManager] %(message)s'))
        self.logging.addHandler(ch)

        try:
            self.ranks = json.loads(open("ranking_cooldown.json", "r+").read())
            self.logging.info("Loaded data file.")
        except FileNotFoundError:
            self.logging.info("File not found. Recreating.")
            now = datetime.timestamp(datetime.now())
            self.ranks = {}
            for tier in tiers:
                if tier in ['MASTER', 'GRANDMASTER', 'CHALLENGER']:
                    self.ranks[(tier, 'I')] = now
                    continue
                for division in divisions:
                    self.ranks[(tier, divisions)] = now
            self.save_to_file()


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
        for key in self.ranks:
            if not oldest_timestamp or self.ranks[key] < oldest_timestamp:
                oldest_key = key
                oldest_timestamp = self.ranks[key]
        self.logging.info(f"Commencing on {oldest_key}.")
        return oldest_key

    async def update(self, key):
        """Update the stats on a rank that is done pulling."""
        self.logging.info(f"Done with {key}.")
        now = datetime.timestamp(datetime.now())
        self.ranks[key] = now
        await self.save_to_file()

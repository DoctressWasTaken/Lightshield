"""League Updater Module."""
import asyncio
import logging

from lightshield.services.league_ranking.service import LeagueRankingService
from lightshield.config import Config
from lightshield.services import HandlerTemplate


class Handler(HandlerTemplate):
    def __init__(self):
        super().__init__()

        self.platforms = {
            platform: LeagueRankingService(platform, self.config)
            for platform in self.config.active_platforms
        }

    async def run(self):
        """Run."""
        await self.init_db()
        self.logging.info("Starting service")
        tasks = []
        for platform in self.platforms.values():
            tasks.append(asyncio.create_task(platform.run(self.db)))

        await asyncio.gather(*tasks)
        await self.cleanup()

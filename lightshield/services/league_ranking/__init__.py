"""League Updater Module."""
import asyncio
import logging

from lightshield.services.league_ranking.rank_manager import RankManager
from lightshield.services.league_ranking.service import Service
from lightshield.config import Config


class Handler:
    db = None

    def __init__(self):
        self.config = Config()
        self.logging = logging.getLogger("Handler")
        self.connection = self.config.get_db_connection()
        self.proxy = self.config.proxy.string
        self.platforms = {
            platform: Service(platform, self.config, self)
            for platform in self.config.active_platforms}

    async def init_shutdown(self, *args, **kwargs):
        """Initiate shutdown."""
        for platform in self.platforms.values():
            await platform.shutdown()

    async def handle_shutdown(self):
        await self.db.close()

    async def run(self):
        """Run."""
        self.db = await self.connection.init()
        tasks = []
        for platform in self.platforms.values():
            tasks.append(asyncio.create_task(platform.run()))

        await asyncio.gather(*tasks)
        await self.handle_shutdown()

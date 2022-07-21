"""League Updater Module."""
import asyncio
import logging

from lightshield.services.league_ranking.rank_manager import RankManager
from lightshield.services.league_ranking.service import Service
from lightshield.config import Config


class Handler:
    is_shutdown = False
    platforms = {}
    db = None

    def __init__(self):
        self.config = Config()
        self.logging = logging.getLogger("Handler")
        self.connection = self.config.get_db_connection()
        self.proxy = self.config.proxy.string
        for platform in [platform for platform, conf in self.config.platforms.items() if not conf['disabled']]:
            self.platforms[platform] = Service(platform, self.config, self)

    async def init_shutdown(self, *args, **kwargs):
        """Initiate shutdown."""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

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

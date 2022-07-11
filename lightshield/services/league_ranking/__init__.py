"""League Updater Module."""
import asyncio
import logging
import os

import asyncpg

from lightshield.services.league_ranking.rank_manager import RankManager
from lightshield.services.league_ranking.service import Service
from lightshield import connection_handler


class Handler:
    is_shutdown = False
    platforms = {}
    db = None

    def __init__(self, config):
        self.config = config
        self.logging = logging.getLogger("Handler")
        self.connection = connection_handler.Connection(config=config)
        self.service = config.services.league_ranking
        self.protocol = config.connections.proxy.protocol
        self.proxy = "%s://%s" % (
            config.connections.proxy.protocol,
            config.connections.proxy.location,
        )

        select_platforms = self.service.platforms or config.statics.enums.platforms
        for platform in select_platforms:
            self.platforms[platform] = Service(platform, config, self)

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

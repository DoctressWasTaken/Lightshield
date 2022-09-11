from lightshield.config import Config
import logging
import asyncio
import aio_pika


class HandlerTemplate:
    db = connection = None
    pika = None
    config = None
    is_shutdown = False
    platforms = {}

    def __init__(self):
        self.config = Config()
        self.logging = logging.getLogger(self.__class__.__name__)

    async def init_db(self):
        """Init connection to the postgres DB."""
        self.connection = self.config.get_db_connection()
        self.db = await self.connection.init()
        self.logging.debug("Initiated connection to postgres.")

    async def init_rabbitmq(self):
        """Init connection to rabbitmq."""
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq._string, loop=asyncio.get_event_loop()
        )

    async def shutdown(self, *args, **kwargs):
        """Initiate shutdown from external signal."""
        for platform in self.platforms.values():
            await platform.shutdown()
        self.is_shutdown = True

    async def cleanup(self):
        """Process shutdown."""
        if self.db:
            await self.db.close()
        if self.pika:
            await self.pika.close()


class ServiceTemplate:
    is_shutdown = False

    def __init__(self, platform, config):
        self.platform = platform
        self.logging = logging.getLogger(self.__class__.__name__ + " | " + platform)
        self.config = config

    async def shutdown(self):
        self.is_shutdown = True


async def fail_loop(func, args, logger, retries=5):

    for _ in range(retries):
        try:
            return await func(*args)
        except Exception as err:
            logger.error(err)

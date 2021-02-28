import os
from contextlib import asynccontextmanager
import asyncio
import asyncpg
import traceback
import logging

class PostgresConnector:

    def __init__(self, user='postgres'):
        self.host = os.environ['PERSISTENT_HOST']
        self.port = int(os.environ['PERSISTENT_PORT'])
        self.db = os.environ['PERSISTENT_DATABASE']
        self.user = user
        self.connection = None
        self.prepare = None
        self.lock = asyncio.Lock()
        self.logging = logging.getLogger("Persistent")
        level = logging.INFO
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(asctime)s [DB Connector] %(message)s"))
        self.logging.addHandler(handler)


    @asynccontextmanager
    async def get_connection(self, exclusive=False):
        if exclusive:
            await self.lock.acquire()
        try:
            if not self.connection or self.connection.is_closed():
                self.connection = await asyncpg.connect(
                    "postgresql://%s@%s:%s/%s" % (
                        self.user, self.host, self.port, self.db))
                if self.prepare:
                    await self.prepare(self.connection)
            yield self.connection
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)
            raise err
        finally:
            if exclusive:
                self.lock.release()

    def set_prepare(self, func):
        """Pass an async function that contains prepared statements to be executed after connect.

        The function itself has to accept the connection parameter.
        """
        self.prepare = func

    async def close(self):
        await self.connection.close()

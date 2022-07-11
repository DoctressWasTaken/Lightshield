import asyncio
import os

import asyncpg
import logging


class Connection:
    db = schema = database = None

    def __init__(self, config):
        self.type = config.database
        self.con_details = getattr(config.connections, self.type)
        self.logging = logging.getLogger("Connection Handler")
        if self.type in ["crate", "postgres"]:
            self.logging.info("Setting up a connection to a %s db.", self.type)
        else:
            self.logging.info("'%s' is not an allowed DB type ('crate', 'postgres').")
            exit()

    async def init(self):
        if self.type == "crate":
            self.schema = self.con_details.schema
            return await asyncpg.create_pool(
                host=self.con_details.hostname,
                port=self.con_details.port,
                user=self.con_details.user,
                password=os.getenv(self.con_details.password_env, None),
            )
        self.database = self.con_details.database
        return await asyncpg.create_pool(
            host=self.con_details.hostname,
            port=self.con_details.port,
            user=self.con_details.user,
            database=self.con_details.database,
            password=os.getenv(self.con_details.password_env, None),
        )

    async def close(self):
        await self.db.close()

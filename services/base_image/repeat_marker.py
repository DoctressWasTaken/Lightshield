"""The repeat marker manages data kept to avoid calling data that should not force a new call.

This is done by using a localized SQLite Database to hold the data.
It provides a connection pool manager to avoid multiple worker overflowing the database.
"""
import aiosqlite
import logging
import asyncio
import os
import socket


class RepeatMarker:
    """Marker class."""

    def __init__(self, connections=1):
        """Set logger and initial elements"""
        self.max_connections = connections

        self.logging = logging.getLogger("RepeatMarker")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [RepeatMarker] %(message)s'))
        self.logging.addHandler(handler)

        self.connection = None
        self.connections = []
        self.write_connection = None

    async def execute_read(self, query):
        """Execute a read query passed on by the client.

        Will await a free connections before executing the query.
        """
        async with self.connection.execute(query) as cursor:
            data = await cursor.fetchall()
            return data

    async def execute_write(self, query):
        """Execute a write query passed on by the client.

        Will await a free connections before executing the query.
        """
        await self.connection.execute(query)
        await self.connection.commit()

    async def connect(self):
        """Create connections to the database.

        Number of connections is determined by the connections parameter supplied in the
        __init__ method.
        """
        dbname = "sqlite/%s_%s.db" % (os.environ['SERVER'], socket.gethostname())
        self.connection = await aiosqlite.connect(dbname)

    async def build(self, query):
        """Try to create SQL tables."""
        dbname = "sqlite/%s_%s.db" % (os.environ['SERVER'], socket.gethostname())
        if not os.path.exists(dbname):
            self.logging.info("No DB File found. Creating.")
            async with aiosqlite.connect(dbname) as db:
                await db.execute(query)
                await db.commit()



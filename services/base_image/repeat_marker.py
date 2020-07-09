"""The repeat marker manages data kept to avoid calling data that should not force a new call.

This is done by using a localized SQLite Database to hold the data.
It provides a connection pool manager to avoid multiple worker overflowing the database.
"""
import aiosqlite
import logging
import asyncio
import os


class RepeatMarker:
    """Marker class."""

    def __init__(self, connections=5):
        """Set logger and initial elements"""
        self.max_connections = connections

        self.logging = logging.getLogger("RepeatMarker")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [RepeatMarker] %(message)s'))
        self.logging.addHandler(handler)

        self.connections = []
        self.write_connection = None
        if not os.path.exists('sqlite3.db'):
            self.logging.info("No 'sqlite3.db' file found. Check your bindings.")
            exit()

    async def execute_read(self, query):
        """Execute a read query passed on by the client.

        Will await a free connections before executing the query.
        """
        while not self.connections:
            await asyncio.sleep(0.1)
        connection = self.connections.pop()
        with await connection.execute(query) as cursor:
            data = await cursor.fetchall()
            await cursor.close()
        self.connections.append(connection)
        return data

    async def execute_write(self, query):
        """Execute a write query passed on by the client.

        Will await a free connections before executing the query.
        """
        while not self.write_connection:
            await asyncio.sleep(0.1)
        connection = self.write_connection
        self.write_connection = None
        await connection.execute(query)
        await connection.commit()
        self.write_connection = connection

    async def connect(self):
        """Create connections to the database.

        Number of connections is determined by the connections parameter supplied in the
        __init__ method.
        """
        self.connections = [await aiosqlite.connect('sqlite3.db') for _ in
                            range(self.max_connections)]
        self.write_connection = await aiosqlite.connect('sqlite3.db')

    async def build(self, query):
        """Try to create SQL tables."""
        async with aiosqlite.connect('sqlite3.db') as db:
            await db.execute(query)
            await db.commit()



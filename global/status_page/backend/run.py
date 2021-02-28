import asyncio
import json
import os
from datetime import datetime
import logging
import asyncpg
from aiohttp import web
import traceback


class Server:
    def __init__(self):
        self.db_host = os.environ["DB_HOST"]
        self.db_database = os.environ["DB_DATABASE"]
        self.server = os.environ["SERVER"].split(",")
        self.last = datetime.now()
        self.cutoff = os.environ["DETAILS_CUTOFF"]

        self.logging = logging.getLogger("Main")
        level = logging.INFO
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))

        self.data = {}

    async def make_app(self):
        app = web.Application()
        app.add_routes([web.get("/api/status", self.return_status)])
        return app

    async def get_data(self, server, conn):
        data = {"server": server, "summoner": {}, "match": {}}
        data["summoner"]["total"] = (
            await conn.fetchval(
                """ 
                        SELECT COUNT(summoner_id)
                                FROM %s.summoner;
                    """
                % server.lower()
            )
            or 0
        )
        data["summoner"]["no_id"] = (
            await conn.fetchval(
                """ 
                        SELECT COUNT(summoner_id)
                                FROM %s.summoner
                                WHERE puuid IS NULL;
                                        """
                % server.lower()
            )
            or 0
        )
        data["summoner"]["no_history"] = (
            await conn.fetchval(
                """ 
                        SELECT COUNT(summoner_id)
                                FROM %s.summoner
                                WHERE wins_last_updated IS NULL;
                    """
                % server.lower()
            )
            or 0
        )
        data["summoner"]["average_delay"] = (
            await conn.fetchval(
                """ 
                        SELECT AVG((wins::float + losses) - (wins_last_updated + losses_last_updated))
                                FROM %s.summoner
                                WHERE wins_last_updated IS NOT NULL;
                    """
                % server.lower()
            )
            or 0
        )
        data["match"]["total"] = (
            await conn.fetchval(
                """ 
                        SELECT COUNT(match_id)
                        FROM %s.match 
                        WHERE DATE(timestamp) >= '%s';
                    """
                % (server.lower(), self.cutoff)
            )
            or 0
        )
        data["match"]["details_missing"] = (
            await conn.fetchval(
                """ 
                        SELECT COUNT(match_id)
                        FROM %s.match 
                        WHERE DATE(timestamp) >= '%s'
                        AND details_pulled IS NULL;
                    """
                % (server.lower(), self.cutoff)
            )
            or 0
        )
        data["match"]["timeline_missing"] = (
            await conn.fetchval(
                """ 
                        SELECT COUNT(match_id)
                        FROM %s.match 
                        WHERE DATE(timestamp) >= '%s'
                        AND timeline_pulled IS NULL;
                    """
                % (server.lower(), self.cutoff)
            )
            or 0
        )
        return data

    async def generate_data(self):
        try:
            while True:
                tasks = []
                for server in self.server:
                    conn = await asyncpg.connect(
                        "postgresql://%s@%s/%s"
                        % (server.lower(), self.db_host, self.db_database.lower())
                    )
                    tasks.append(await self.get_data(server, conn))
                    await conn.close()
                self.data = tasks
                self.logging.info(self.data)
                await asyncio.sleep(60)
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)
            exit()

    async def return_status(self, request):
        return web.Response(
            text=json.dumps(self.data), headers={"Access-Control-Allow-Origin": "*"}
        )


async def main():
    server = Server()
    await asyncio.gather(server.generate_data())


if __name__ == "__main__":
    asyncio.run(main())

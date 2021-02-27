import asyncio
import json
import os
from datetime import datetime

import asyncpg
from aiohttp import web


class Server:
    def __init__(self):
        self.db_host = os.environ["DB_HOST"]
        self.db_database = os.environ["DB_DATABASE"]
        self.server = os.environ["SERVER"].split(",")
        self.last = datetime.now()
        self.cutoff = os.environ["DETAILS_CUTOFF"]

        self.data = None
        with open("status.json", "r") as data:
            self.data = json.loads(data.read())

    async def make_app(self):
        app = web.Application()
        app.add_routes([web.get("/api/status", self.return_status)])
        return app

    async def get_data(self, server):
        conn = await asyncpg.connect(
            "postgresql://%s@%s/%s"
            % (server.lower(), self.db_host, self.db_database.lower())
        )
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
                await conn.fetch(
                    """ 
                    SELECT COUNT(match_id),
                    FROM %s.match 
                    WHERE DATE(timestamp) >= '%s';
                """
                    % (server.lower(), self.cutoff)
                )
                or 0
        )
        data["match"]["details_missing"] = (
                await conn.fetch(
                    """ 
                    SELECT COUNT(match_id),
                    FROM %s.match 
                    WHERE DATE(timestamp) >= '%s'
                    AND details_pulled IS NULL;
                """
                    % (server.lower(), self.cutoff)
                )
                or 0
        )
        data["match"]["timeline_missing"] = (
                await conn.fetch(
                    """ 
                    SELECT COUNT(match_id),
                    FROM %s.match 
                    WHERE DATE(timestamp) >= '%s'
                    AND timeline_pulled IS NULL;
                """
                    % (server.lower(), self.cutoff)
                )
                or 0
        )
        await conn.close()
        return data

    async def generate_data(self, repeat=0):
        tasks = await asyncio.gather(*[self.get_data(server) for server in self.server])

        self.data = tasks

        while repeat > 0:
            tasks = await asyncio.gather(
                *[self.get_data(server) for server in self.server]
            )

            with open("status.json", "w+") as data:
                data.write(json.dumps(tasks))
            await asyncio.sleep(repeat)

    async def return_status(self, request):
        return web.Response(
            json=self.data, headers={"Access-Control-Allow-Origin": "*"}
        )


async def main():
    server = Server()
    await server.generate_data()
    await asyncio.gather(
        web._run_app(server.make_app(), port=8000), server.generate_data(repeat=60)
    )


if __name__ == "__main__":
    asyncio.run(main())

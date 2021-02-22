import asyncio
import json
import os
from datetime import datetime, timedelta

import asyncpg
from aiohttp import web


class Server:

    def __init__(self):
        self.db_host = os.environ['DB_HOST']
        self.server = os.environ['SERVER'].split(',')
        self.last = datetime.now()
        self.cutoff = os.environ['DETAILS_CUTOFF']

    async def make_app(self):
        app = web.Application()
        app.add_routes([
            web.get('/api/status', self.return_status)
        ])

    async def get_data(self, server):
        conn = await asyncpg.connect(
            "postgresql://%s@%s/%s" % (server.lower(), self.db_host, server.lower()))
        data = {"server": server}
        res = await conn.fetch(''' 
            SELECT COUNT(summoner_id),
                    CASE WHEN puuid IS NULL THEN 'outstanding'
                    ELSE 'pulled' END AS status
                    FROM summoner
                    GROUP BY status;
        ''')
        data['summoner'] = {}
        for line in res:
            data['summoner'][line['status']] = line['count']

        res = await conn.fetch(''' 
            SELECT COUNT(match_id),
            CASE WHEN details_pulled IS NULL THEN 'outstanding'
            ELSE 'pulled' END AS status
            FROM match WHERE DATE(timestamp) >= '%s'
            GROUP BY status;
        ''' % self.cutoff)
        data['match'] = {}
        for line in res:
            data['match'][line['status']] = line['count']
        await conn.close()
        return data

    async def generate_file(self):
        tasks = await asyncio.gather(*[
            self.get_data(server) for server in self.server
        ])

        with open('status.json', 'w+') as data:
            data.write(json.dumps(tasks))

    async def return_status(self):
        if self.last + timedelta(seconds=15) < datetime.now():
            await self.generate_file()
        with open('status.json', 'r').read() as data:
            return web.Response(text=data, headers={'Access-Control-Allow-Orogin': 'lightshield.dev'})


async def start_gunicorn():
    """Return webserver element to gunicorn.

    Called by gunicorn directly.
    """
    server = Server()
    await server.generate_file()
    return await server.make_app()

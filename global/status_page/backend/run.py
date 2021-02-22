import asyncio
import json
import logging
import os
import threading
from datetime import datetime

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
        return app

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
        if not 'outstanding' in data['summoner']:
            data['summoner']['outstanding'] = 0

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
        if not 'outstanding' in data['match']:
            data['match']['outstanding'] = 0

        await conn.close()
        return data

    async def generate_file(self, repeat=0):
        tasks = await asyncio.gather(*[
            self.get_data(server) for server in self.server
        ])

        with open('status.json', 'w+') as data:
            logging.info(tasks)
            data.write(json.dumps(tasks))

        while repeat > 0:
            tasks = await asyncio.gather(*[
                self.get_data(server) for server in self.server
            ])

            with open('status.json', 'w+') as data:
                logging.info(tasks)
                data.write(json.dumps(tasks))
            await asyncio.sleep(repeat)

    async def return_status(self, request):
        print("requested")
        with open('status.json', 'r') as data:
            return web.Response(text=data.read(), headers={'Access-Control-Allow-Origin': '*'})


async def start_gunicorn():
    """Return webserver element to gunicorn.

    Called by gunicorn directly.
    """
    server = Server()
    await server.generate_file()
    task = asyncio.create_task(server.generate_file(repeat=60))
    return await server.make_app()


def updater():
    asyncio.run(server.generate_file(repeat=60))


if __name__ == '__main__':
    server = Server()
    asyncio.run(server.generate_file())
    t = threading.Thread(target=updater)
    t.start()
    web.run_app(server.make_app(), port=8000)

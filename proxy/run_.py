import asyncio
from aiohttp import web

from datetime import timezone, datetime
import pytz
import os, json
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)



count = 0

class Limit:

    def __init__(self, span, count):
        self.span = int(span)
        self.max = count
        self.count = 0
        self.bucket = datetime.now(timezone.utc)
        self.last_updated = datetime.now(timezone.utc)
        self.blocked = datetime.now(timezone.utc)
        print(f"Initiated {self.max}:{self.span}.")

class ApiHandler:

    def __init__(self):
        limits = json.loads(open("limits.json").read())
        self.globals = []
        for limit in limits['APP']:
            self.globals.append(
                    Limit(limit, limits['APP'][limit]))
        self.methods = {}
        for method in limits['METHODS']:
            for limit in limits['METHODS'][method]:
                self.methods[method] = Limit(limit, limits['METHODS'][method][limit])

    def request:


class Proxy:

    def __init__(self):
        self.api = ApiHandler()
        self.app = web.Application()
        self.app.add_routes([
            web.get('/league-exp/{tail:.*}', self.league)
            ])
        
    def run(self, host="0.0.0.0", port=8080):
        web.run_app(self.app, port=port, host=host)


    async def league(self, request):
        print(request.rel_url)
        return web.Response(text="test")

async def league(request):
    global count
    print(request.rel_url)
    if count < 10:
        print("Setting count to", count + 1)
        count += 1
    else:
        print("Skipping")
        return web.Response(text="Its full")
    await asyncio.sleep(2)
    print("Done", request.rel_url)
    return web.Response(text="Hello World")



if __name__ == '__main__':
    proxy = Proxy()
    proxy.run(port=8888, host="localhost")


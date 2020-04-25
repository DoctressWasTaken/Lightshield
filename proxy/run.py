import asyncio
from aiohttp import web
import aiohttp

from datetime import timezone, datetime, timedelta
import pytz
import os
import json
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

if "SERVER" not in os.environ:
    logging.info("No SERVER env variable provided. Exiting.")
    exit()

config = json.loads(open("config.json").read())

if not "SERVER" in os.environ:
    logging.error("No SERVER env variable provided. Exiting.")
    exit()
server = os.environ["SERVER"]

headers = {'X-Riot-Token': config['API_KEY']}
base_url = f"https://{server}.api.riotgames.com/lol"


count = 0

class Limit:

    def __init__(self, span, count):
        self.span = int(span) + 1
        self.max = count
        self.count = 0
        self.bucket = datetime.now(timezone.utc)
        self.last_updated = datetime.now(timezone.utc)
        self.blocked = datetime.now(timezone.utc)
        print(f"Initiated {self.max}:{self.span}.")

    async def is_blocked(self):
        print(self.count, self.max)
        if self.blocked > datetime.now(timezone.utc):
            return True

        if self.bucket < datetime.now(timezone.utc):  # Bucket timed out
            self.count = 0
            return False
        if self.count < self.max:  # Not maxed out
            return False
        return True

    async def register(self):

        if self.blocked > datetime.now(timezone.utc):
            logging.info("Limit is blocked.")
            return False

        if self.bucket < datetime.now(timezone.utc):
            self.bucket = datetime.now(timezone.utc) + timedelta(seconds=self.span)
            logging.info(f"Starting bucket until {self.bucket}.")
            self.count = 1
            return True

        if self.count < self.max:
            self.count += 1
            logging.info(f"Adding request {self.count} : {self.max}.")
            return True

        logging.info("Already on limit")
        return False

    async def sync(self, current, date, retry_after):
        if self.last_updated > date:
            return
        if count > self.count:
            self.count = count
        if self.count > self.max:
            self.blocked = datetime.now(timezone.utc) + timedelta(seconds=retry_after)
            print("Blocking for", retry_after)


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

    async def request(self, url, method):

        if await self.methods[method].is_blocked():
            print("Method limit is blocking")
            return 428, {}
        print("Method limit not blocking")
        for limit in self.globals:
            if await limit.is_blocked():
                print("Global limit is blocking")
                return 428, {}
        print("Global limit not blocking")

        if not await self.methods[method].register():
            print("Method can't register")
            return 428, {}
        print("Method registered")
        for limit in self.globals:
            if not await limit.register():
                print("Global can't register")
                return 428, {}
        print("Global registered")

        print("\n\t\t\t\t\t\tALLOWED REQUEST\n")
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url + url, headers=headers) as resp:
                body = await resp.json()
                print(resp.status)
                if resp.status != 200:
                    print(body)
                    print(resp.headers)

                app_current = resp.headers['X-App-Rate-Limit-Count']
                method_current = resp.headers['X-Method-Rate-Limit-Count']
                naive = datetime.strptime(
                    resp.headers['Date'],
                    '%a, %d %b %Y %H:%M:%S GMT')
                local = pytz.timezone('GMT')
                local_dt = local.localize(naive, is_dst=None)
                date = local_dt.astimezone(pytz.utc)

                retry_after = 0
                if 'Retry-After' in resp.headers:
                    retry_after = int(resp.headers['Retry-After'])

                for limit in self.globals:
                    for current in app_current.split(','):
                        if current.endswith(str(limit.span)):
                            await limit.sync(
                                int(current.split(":")[0]),
                                date,
                                retry_after)
                            break
                await self.methods[method].sync(
                    int(method_current.split(":")[0]),
                    date,
                    retry_after)
        return 200, {}

class Proxy:

    def __init__(self):
        self.api = ApiHandler()
        self.app = web.Application()
        self.app.add_routes([
            web.get('/league-exp/{tail:.*}', self.league_exp),
            web.get('/summoner/{tail:.*}', self.summoner),

        ])
        
    def run(self, host="0.0.0.0", port=8080):
        web.run_app(self.app, port=port, host=host)


    async def league_exp(self, request):
        response = await self.api.request(str(request.rel_url), "league-exp")
        return web.Response(text=json.dumps(response[1]), status=response[0])

    async def summoner(self, request):
        response = await self.api.request(str(request.rel_url), "summoner")
        return web.Response(text=json.dumps(response[1]), status=response[0])


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
    proxy.run(port=8000, host="0.0.0.0")


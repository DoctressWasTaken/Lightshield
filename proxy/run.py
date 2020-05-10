# -*- coding: utf-8 -*-
"""Proxy Module.

Routes and ratelimits all calls to the API.
"""
import asyncio  # noqa: F401
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
server = os.environ["SERVER"]

headers = {'X-Riot-Token': config['API_KEY']}
base_url = f"https://{server}.api.riotgames.com/lol"
count = 0


class Limit:
    """Class that creates objects for each limit identified.

    Handles tracking of rate limits for this specific limit.
    """

    def __init__(self, span, count):
        """Initialize Limit data.

        :arg::span::The duration of the bucket applied to the Limit.
            The duration is automatically extended by 1 second.
        :arg::count::The maximal requests in a single bucket.
        """
        self.span = int(span) + 1
        self.max = count
        self.count = 0
        self.bucket = datetime.now(timezone.utc)
        self.last_updated = datetime.now(timezone.utc)
        self.blocked = datetime.now(timezone.utc)
        print(f"Initiated {self.max}:{self.span}.")

    async def is_blocked(self):
        """Return if a limit is blocked."""
        if self.blocked > datetime.now(timezone.utc):
            return True

        if self.bucket < datetime.now(timezone.utc):  # Bucket timed out
            self.count = 0
            return False
        if self.count < self.max:  # Not maxed out
            return False
        return True

    async def register(self):
        """Reqister a request.

        Returns false if the limit is blocked, true if open.
        Adds count to limit when returning true.
        While iterating through applied limits,
        if a later limit blocks despite previous being open,
        the previous limit counts are NOT lowered.
        """
        if self.blocked > datetime.now(timezone.utc):
            logging.info("Limit is blocked.")
            return False

        if self.bucket < datetime.now(timezone.utc):
            self.bucket = datetime.now(timezone.utc) + timedelta(
                seconds=self.span)
            logging.info(f"Starting bucket until {self.bucket}.")
            self.count = 1
            return True

        if self.count < self.max:
            self.count += 1
            if self.max * 0.9 < self.count:
                logging.info(f"Adding request {self.count} : {self.max}.")
            return True

        return False

    async def sync(self, current, date, retry_after):
        """Syncronize the locally kept API limits with a request response."""
        if self.last_updated > date:
            return
        if count > self.count:
            self.count = count
        if self.count > self.max:
            self.blocked = datetime.now(timezone.utc) + timedelta(
                seconds=retry_after)


class ApiHandler:
    """Class to manage currently used rate limits on user side.

    Requests are checked against both global (app) and method limits before
    being allowed.

    Returning requests update the currently tracked limits
    if a difference is noticed.
    """

    def __init__(self):
        """Init method to set up limits based on an external data file.

        Limits are stored as Limit() Class objects due to them having multiple
        functions.
        Limits are stored in 2 categories for global (app) limits
        and method limits.
        """
        limits = json.loads(open("limits.json").read())
        self.globals = []
        for limit in limits['APP']:
            self.globals.append(
                Limit(limit, limits['APP'][limit]))
        self.methods = {}
        for method in limits['METHODS']:
            for limit in limits['METHODS'][method]:
                self.methods[method] = Limit(limit,
                                             limits['METHODS'][method][limit])

    async def request(self, url, method):
        """Request attempt to the API.

        If both, global and method, limts are not blocked,
        the request is processed.

        Returns Status code according to the case:
        Local Ratelimit applies: 428
        Else: Code returned by the API
        """
        if await self.methods[method].is_blocked():
            return 428, {"error": "error"}
        for limit in self.globals:
            if await limit.is_blocked():
                print("Global limit is blocking")
                return 428, {"error": "error"}

        if not await self.methods[method].register():
            return 428, {"error": "error"}
        for limit in self.globals:
            if not await limit.register():
                return 428, {"error": "error"}

        async with aiohttp.ClientSession() as session:
            async with session.get(base_url + url, headers=headers) as resp:
                body = await resp.json()
                if resp.status != 200:
                    print("Non-200 RESPONSE:")
                    print(resp.status)
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
        if resp.status != 200:
            return resp.status, {"error": "error"}

        return 200, body


class Proxy:
    """The proxy class contains the proxy server to be run.

    Creates an API Handler object for rate limiting.
    run() to start the webserver.
    """

    def __init__(self):
        """Set external objects and routes."""
        self.api = ApiHandler()
        self.app = web.Application()
        self.app.add_routes([
            web.get('/league-exp/{tail:.*}', self.league_exp),
            web.get('/summoner/{tail:.*}', self.summoner),
            web.get('/match/{tail:.*}', self.match),
            web.get('/{tail:.*}', self.error)

        ])

    def run(self, host="0.0.0.0", port=8080):
        """Run Method.

        Called externally to start the proxy webserver.
        """
        web.run_app(self.app, port=port, host=host)

    async def league_exp(self, request):
        """Process the League-Exp API endpoint.

        Returns the data returned from the api.
        If locally rate limited returns 428 instead.
        """
        response = await self.api.request(str(request.rel_url), "league-exp")
        return web.Response(text=json.dumps(response[1]), status=response[0])

    async def summoner(self, request):
        """Process the Summoner API endpoint.

        Returns the data returned from the api.
        If locally rate limited returns 428 instead.
        """
        response = await self.api.request(str(request.rel_url), "summoner")
        return web.Response(text=json.dumps(response[1]), status=response[0])

    async def match(self, request):
        """Process the Match API endpoint.

        Returns the data returned from the api.
        If locally rate limited returns 428 instead.
        """
        response = await self.api.request(str(request.rel_url), "match")
        return web.Response(text=json.dumps(response[1]), status=response[0])

    async def error(self, request):
        """Return message when non-supported endpoint is called.

        Returns a 500 error.
        """
        print("Received message that could not be processed")
        print(request.rel_url)
        return web.Response(text="error", status=500)


if __name__ == '__main__':
    proxy = Proxy()
    proxy.run(port=8000, host="0.0.0.0")

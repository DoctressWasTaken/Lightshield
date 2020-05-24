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
import concurrent.futures
import time

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

aio = logging.getLogger("aiohttp")
aio.setLevel(logging.ERROR)

"""
if "SERVER" not in os.environ:
    logging.info("No SERVER env variable provided. Exiting.")
    exit()
if "API_KEY" not in os.environ:
    logging.info("No API_KEY env variable provided. Exiting.")
    exit()

server = os.environ["SERVER"]
headers = {'X-Riot-Token': os.environ['API_KEY']}
"""
headers = ""
server = ""

base_url = f"https://{server}.api.riotgames.com/lol"
count = 0


class LimitBlocked(Exception):

    def __init__(self, retry_after):
        self.retry_after = retry_after


class Limit:
    """Class that creates objects for each limit identified.

    Handles tracking of rate limits for this specific limit.
    """

    def __init__(self, span, max, name):
        """Initialize Limit data.

        :arg::span::The duration of the bucket applied to the Limit.
            The duration is automatically extended by 1 second.
        :arg::max::The maximal requests in a single bucket.
        """
        self.span = int(span)  # Duration of the bucket
        self.max = max - 1  # Max Calls per bucket (Reduced by 1 for safety measures)
        self.count = 0  # Current Calls in this bucket
        self.bucket = datetime.now(
            timezone.utc)  # Cutoff after which no new requests are accepted
        self.reset = self.bucket + timedelta(
            seconds=1)  # Moment of reset for the bucket
        self.last_updated = datetime.now(
            timezone.utc)  # Latest received response
        self.blocked = datetime.now(timezone.utc)
        print(f"[{name.capitalize()}] Initiated {self.max}:{self.span}.")

    async def check_reset(self):
        """Check if the bucket has timed out."""
        if datetime.now(timezone.utc) > self.reset:
            self.count = 0
            self.bucket = datetime.now(timezone.utc) + timedelta(
                seconds=self.span - 1)
            self.reset = datetime.now(timezone.utc) + timedelta(
                seconds=self.span)

    async def accept_requests(self):
        """Check if the bucket is currently accepting requests."""
        if self.bucket < datetime.now(timezone.utc) < self.reset:
            raise LimitBlocked(await self.when_reset())
        return True

    async def when_reset(self):
        """Return seconds until reset."""
        return (self.reset - datetime.now(timezone.utc)).total_seconds()

    async def register(self):
        """Reqister a request.

        Returns false if the limit is blocked, true if open.
        Adds count to limit when returning true.
        While iterating through applied limits,
        if a later limit blocks despite previous being open,
        the previous limit counts are NOT lowered.
        """
        await self.check_reset()
        # Return if in dead-period
        await self.accept_requests()

        # Attempt to add request
        if self.count >= self.max:
            raise LimitBlocked(await self.when_reset())
        self.count += 1

    async def sync(self, headers, type):
        """Syncronize the locally kept API limits with a request response."""
        naive = datetime.strptime(
            headers['Date'],
            '%a, %d %b %Y %H:%M:%S GMT')
        local = pytz.timezone('GMT')
        local_dt = local.localize(naive, is_dst=None)
        date = local_dt.astimezone(pytz.utc)
        if self.last_updated > date:
            return

        if type == "global":
            limits = headers['X-App-Rate-Limit-Count'].split(',')
            for limit in limits:
                if limit.split(":")[1] == str(self.span):
                    current = int(limit.split(":")[0])
                    break
        else:
            limits = headers['X-Method-Rate-Limit-Count']
            current = int(limits.split(":")[0])
        if not current:
            print("The approriated limit could not be found.")
            return

        if current > self.count:
            self.count = current
        if current < self.max:
            return
        self.bucket = datetime.now(timezone.utc)

        retry_after = 1
        if 'Retry_After' in headers:
            retry_after = int(headers['Retry-After'])
        self.reset = datetime.now(timezone.utc) + timedelta(
            seconds=retry_after + 1)


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
                Limit(limit, limits['APP'][limit], 'App'))
        self.methods = {}
        for method in limits['METHODS']:
            for limit in limits['METHODS'][method]:
                self.methods[method] = Limit(limit,
                                             limits['METHODS'][method][limit],
                                             method)

    def fetch(self, session, url):
        global count
        count += 1
        # print(count)
        #print(datetime.now())
        time.sleep(0.8)
        return 0

    async def request(self, url, method):
        """Request attempt to the API.

        If both, global and method, limts are not blocked,
        the request is processed.

        Returns Status code according to the case:
        Local Ratelimit applies: 428
        Else: Code returned by the API
        """
        try:
            pass
            #await self.methods[method].register()
            #for limit in self.globals:
            #    await limit.register()
        except LimitBlocked as err:
            return 428, {"status": "locally_blocked",
                         "retry_after": err.retry_after}
        loop = asyncio.get_running_loop()
        try:
            async with aiohttp.ClientSession() as session:
                with concurrent.futures.ProcessPoolExecutor() as pool:
                    await loop.run_in_executor(
                        pool,
                        self.fetch, session, url)
            return 200, {"status": "Teapot"}

        except aiohttp.ClientOSError as err:
            print(err)
            return 201, {"status": "could not connect"}


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

    proxy.run(port=8000, host="127.0.0.1")

import aioredis
import aiohttp
import asyncio
import yaml
import logging
import re
import pprint
import os
from datetime import datetime, timedelta
import hashlib

pp = pprint.PrettyPrinter(indent=4)
logging.basicConfig()

from aiohttp import web


class Mapping:
    url_regex = re.compile("https?:\/\/([a-z12]{2,8}).riotgames.com(.*)")
    endpoints = {}
    session = redis = permit = update = None

    def __init__(self):
        with open("endpoints.yaml") as endpoints:
            data = yaml.safe_load(endpoints)
            ep = data["endpoints"]
            placeholders = data["placeholders"]
            pp.pprint(placeholders)
            for cat in ep:
                for endpoint in ep[cat]:
                    regex = endpoint.replace("/", "\/")
                    for plc in re.findall("\{([a-zA-Z]*)}", endpoint):
                        regex = regex.replace(
                            "{%s}" % plc, placeholders[plc].replace("/", "\\/")
                        )
                    self.endpoints[regex] = endpoint
        # pp.pprint(self.endpoints)

    async def init(self, host="localhost", port=6379):
        self.session = aiohttp.ClientSession(
            headers={"X-Riot-Token": os.environ.get("API_KEY")}
        )
        self.redis = aioredis.from_url(
            "redis://%s:%s" % (host, port), encoding="utf-8", decode_responses=True
        )
        # TODO: If multiple copies of the service are started this might lead to multiple scripts
        #   And with it to parallel executions of said script (unsure)
        with open("scripts/permit_handler.lua") as permit:
            self.permit = await self.redis.script_load(permit.read())
        with open("scripts/update_ratelimits.lua") as update:
            self.update = await self.redis.script_load(update.read())

    @web.middleware
    async def middleware(self, request, handler):
        url = str(request.url)
        print()
        print("Received Url:", url)
        server, path = self.url_regex.findall(url)[0]
        endpoint = None
        for ep in self.endpoints:
            match = re.fullmatch(ep, path)
            if not match:
                continue
            endpoint = ep
        if not endpoint:
            print("There was an error recognizing the endpoint for %s.")
            return web.json_response({"error": "Endpoint not recognized."}, status=404)

        send_timestamp = int(datetime.now().timestamp())
        request_string = "%s_%s" % (url, send_timestamp)
        request_id = hashlib.md5(request_string.encode()).hexdigest()
        print(request_id)
        print("Endpoint:", self.endpoints[endpoint])
        print("Regex:", endpoint)
        await self.pre_flight(server, endpoint, send_timestamp, request_id)

        return web.json_response({"empty": "empty"})
        # async with aiohttp.ClientSession() as session:
        #     async with session.get(request.url) as resp:
        #         return web.Response(text=await resp.text())

    async def pre_flight(self, server, endpoint, send_timestamp, request_id):
        """Make sure that the request is allowed to proceed."""
        time_to_wait = await self.redis.evalsha(
            self.permit, 2, server, endpoint, send_timestamp, request_id  # Keys
        )  # Args

    async def pseudo_handler(request: web.Request) -> web.Response:
        """Pseudo handler that should never be called."""
        return web.Response(text="Something went wrong")


async def init_app():
    mapping = Mapping()
    app = web.Application(middlewares=[mapping.middleware])
    app.add_routes([web.get("/", mapping.pseudo_handler)])
    return app


app = asyncio.run(init_app())
web.run_app(app, host="127.0.0.1", port=8888)

import aioredis
import aiohttp
import asyncio
import yaml
import logging
import re
import pprint
import os
from datetime import datetime
import hashlib

pp = pprint.PrettyPrinter(indent=4)

if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )

logging.getLogger().setLevel(logging.WARN)
from aiohttp import web


class Mapping:
    url_regex = re.compile("https?:\/\/([a-z12]{2,8}).api.riotgames.com(.*)")
    endpoints = {}
    session = redis = permit = update = None

    def __init__(self):
        self.logging = logging.getLogger("Mapping")
        self.logging.setLevel(logging.INFO)
        with open("endpoints.yaml") as endpoints:
            data = yaml.safe_load(endpoints)
            ep = data["endpoints"]
            placeholders = data["placeholders"]
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
            self.logging.info(self.permit)
        with open("scripts/update_ratelimits.lua") as update:
            self.update = await self.redis.script_load(update.read())
            self.logging.info(self.update)

    @web.middleware
    async def middleware(self, request, handler):
        try:
            assert self.session
        except:
            await self.init(
                host=os.environ.get("REDIS_HOST"),
                port=int(os.environ.get("REDIS_PORT")),
            )

        url = str(request.url)
        server, path = self.url_regex.findall(url)[0]
        endpoint = None
        for ep in self.endpoints:
            match = re.fullmatch(ep, path)
            if not match:
                continue
            endpoint = ep
        if not endpoint:
            self.logging.info("There was an error recognizing the endpoint for %s.")
            return web.json_response({"error": "Endpoint not recognized."}, status=404)

        send_timestamp = datetime.now().timestamp() * 1000
        request_string = "%s_%s" % (url, send_timestamp)
        request_id = hashlib.md5(request_string.encode()).hexdigest()
        wait_time = await self.redis.evalsha(
            self.permit, 2, server, endpoint, send_timestamp, request_id
        )
        if wait_time > 0:
            return web.json_response({"Retry-At": wait_time / 1000}, status=430)

        url = url.replace("http:", "https:")
        async with self.session.get(url) as response:
            headers = response.headers
            app_limits = None
            if app_limits := headers.get("X-App-Rate-Limit"):
                app_limits = app_limits.split(",")[0].split(":")
            method_limits = None
            if method_limits := headers.get("X-Method-Rate-Limit"):
                method_limits = method_limits.split(",")[0].split(":")
            if app_limits and method_limits:
                await self.redis.evalsha(
                    self.update,
                    2,
                    server,
                    endpoint,
                    *[int(x) for x in app_limits + method_limits],
                )
            if response.status != 200:
                return web.json_response({}, status=response.status)
            result = await response.json()
            return web.json_response(result)

    async def pseudo_handler(request: web.Request) -> web.Response:
        """Pseudo handler that should never be called."""
        return web.Response(text="Something went wrong")


async def init_app():
    mapping = Mapping()
    app = web.Application(middlewares=[mapping.middleware])
    app.add_routes([web.get("/", mapping.pseudo_handler)])
    return app


async def runner():
    app = await init_app()
    web.run_app(app, host="0.0.0.0", port=8888)


if __name__ == "__main__":
    app = asyncio.run(runner())

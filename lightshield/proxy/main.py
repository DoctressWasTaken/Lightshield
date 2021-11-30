import logging
import re

import aioredis

from .endpoint import Endpoint
from .layers import Server, Zone

pattern = "https://([\w\d]*)\.api\.riotgames\.com(/[^/]*/[^/]*/[v\d]*/[^/]+).*"
compiled = re.compile(pattern)


class Proxy:
    """Central proxy element to be imported."""

    def __init__(self, server_first=True):

        self.redis = None
        self.endpoints = {}
        self.server_first = server_first
        self.logging = logging.getLogger("Proxy")

    async def init(self, host="localhost", port=6379, namespace="ratelimiter"):
        self.redis = await aioredis.create_redis_pool((host, port), encoding="utf-8")
        self.namespace = namespace

    async def request(self, url, session):
        """Request an url."""
        server, zone = compiled.findall(url)[0]
        limit_key = "%s:%s" % (server, zone)
        try:
            return await self.endpoints[limit_key].request(url, session)
        except KeyError:
            if self.server_first:
                order = [
                    Server(server, self.namespace),
                    Zone(server, zone, self.namespace),
                ]
            else:
                order = [
                    Zone(server, zone, self.namespace),
                    Server(server, self.namespace),
                ]
            self.endpoints[limit_key] = Endpoint(
                server, zone, self.redis, order
            )
            return await self.endpoints[limit_key].request(url, session)

    async def get_endpoint(self, url):
        """Return the endpoint used by a provided url."""
        server, zone = compiled.findall(url)[0]
        limit_key = "%s:%s" % (server, zone)
        try:
            return self.endpoints[limit_key]
        except KeyError:
            if self.server_first:
                order = [
                    Server(server, self.namespace),
                    Zone(server, zone, self.namespace),
                ]
            else:
                order = [
                    Zone(server, zone, self.namespace),
                    Server(server, self.namespace),
                ]
            self.endpoints[limit_key] = Endpoint(
                server, zone, self.redis, order
            )
            return self.endpoints[limit_key]

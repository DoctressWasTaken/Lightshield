import logging
import re

import aioredis

from .endpoint import Endpoint

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
        self.redis = aioredis.from_url("redis://%s:%s" % (host, port), encoding="utf-8", decode_responses=True)
        self.namespace = namespace

    async def get_endpoint(self, server, zone):
        """Return the endpoint used by a provided url."""
        limit_key = "%s:%s" % (server, zone)
        try:
            return self.endpoints[limit_key]
        except KeyError:
            endpoint = Endpoint(server, zone, self.redis, self.namespace)
            await endpoint.init()
            self.endpoints[limit_key] = endpoint
            return endpoint

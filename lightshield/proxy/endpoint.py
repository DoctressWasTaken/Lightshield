import logging
from datetime import datetime

from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)


class Endpoint:
    """Handle requests for a specific endpoint."""

    def __init__(self, server, zone, redis, namespace):
        self.server = server
        self.zone = zone
        self.namespace = namespace

        self.redis = redis
        self.logging = logging.getLogger("Proxy")

        self.knows_server = False
        self.knows_zone = False

        self.permit = None
        self.align = None

    async def init(self):
        await self.redis.setnx(
            "%s:%s:%s" % (self.namespace, self.server, self.zone), "1:7"
        )
        await self.redis.setnx("%s:%s" % (self.namespace, self.server), "1:7")
        self.permit = await self.redis.get("lightshield_permit")
        self.align = await self.redis.get("lightshield_update")

    async def request(self, url, session):

        request_stamp = int(datetime.now().timestamp() * 1000)
        if (
            response := await self.redis.evalsha(
                self.permit,
                [
                    "%s:%s:%s" % (self.namespace, self.server, self.zone),
                    "%s:%s" % (self.namespace, self.server),
                ],
                [
                    request_stamp,
                ],
            )
            > 0
        ):
            raise LimitBlocked(retry_after=response)
        async with session.get(url) as response:
            response_json = await response.json()
            status = response.status
            headers = response.headers
        if "X-App-Rate-Limit" in headers and "X-Method-Rate-Limit" in headers:
            await self.redis.evalsha(
                self.align,
                [
                    "%s:%s:%s" % (self.namespace, self.server, self.zone),
                    "%s:%s" % (self.namespace, self.server),
                ],
                [
                    request_stamp,
                    headers.get("X-Method-Rate-Limit"),
                    headers.get("X-Method-Rate-Limit-Count"),
                    headers.get("X-App-Rate-Limit"),
                    headers.get("X-App-Rate-Limit-Count"),
                ],
            )
            if status == 200:
                return response_json
            if status == 404:
                raise NotFoundException()
            if status == 429:
                raise RatelimitException(retry_after=headers.get("Retry-After", 1))
            raise Non200Exception()

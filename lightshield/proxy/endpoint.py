import asyncio
import logging
from datetime import datetime, timedelta

from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)


class Endpoint:
    """Handle requests for a specific endpoint."""

    blocked_until = None

    def __init__(self, server, zone, redis, namespace):
        self.server = server
        self.zone = zone
        self.namespace = namespace

        self.redis = redis
        self.logging = logging.getLogger("Proxy")

        self.key_server = "%s:%s" % (self.namespace, self.server)
        self.key_zone = "%s:%s:%s" % (self.namespace, self.server, self.zone)
        self.logging.info("Server: %s, Zone: %s", self.key_server, self.key_zone)

        self.permit = None
        self.update = None

    async def init(self):
        self.permit = await self.redis.get("lightshield_permit_handler")
        self.limits_init = await self.redis.get("lightshield_limits_init")
        self.limits_drop = await self.redis.get("lightshield_limits_drop")
        self.limits_update = await self.redis.get("lightshield_limits_update")
        self.update = await self.redis.get("lightshield_update_single")

        await self.redis.hsetnx(self.key_server, "placeholder", "H")
        await self.redis.hsetnx(self.key_zone, "placeholder", "H")
        self.logging.info("Initialized")

    async def response(
        self, local_limits, key, header_limits, header_counts, request_timestamp
    ):
        # Changes to limits
        to_init = []
        to_drop = []
        to_update = []

        # Changes to limit count
        updates = []

        limits = {}
        for limit in local_limits:
            limits[int(limit)] = {
                "found": False,
                "count": None,
                "max": None,
                "old_max": int(local_limits[limit]),
                "preexisting": True,
            }

        for count_string in header_counts.split(","):
            count, span = [int(el) for el in count_string.split(":")]
            if not span in limits:
                limits[span] = {"preexisting": False}
            limits[span]["count"] = count
            limits[span]["found"] = True

        for limit_string in header_limits.split(","):
            max, span = [int(el) for el in limit_string.split(":")]
            limits[span]["max"] = max

        for span, val in limits.items():
            if not val["preexisting"]:
                to_init += [val["max"], span]
            elif not val["found"]:
                to_drop.append(span)
            elif val["max"] != val["old_max"]:
                to_update += [val["max"], span]

            if val["found"]:
                updates.append(
                    [
                        self.update,
                        2,
                        "%s:%s" % (key, span),
                        "%s:%s:meta" % (key, span),
                        request_timestamp,
                        val["count"],
                    ]
                )
        return to_init, to_update, to_drop, updates

    async def request(self, url, session, no_block=True):
        """Initiate a request through the proxy."""

        if self.blocked_until:
            while datetime.now() < self.blocked_until:
                if no_block:
                    raise LimitBlocked(
                        retry_after=(
                            self.blocked_until - datetime.now()
                        ).total_seconds()
                    )
                await asyncio.sleep(0.01)
            self.blocked_until = None

        server_limits = await self.redis.hgetall(self.key_server)
        del server_limits["placeholder"]
        zone_limits = await self.redis.hgetall(self.key_zone)
        del zone_limits["placeholder"]
        if not server_limits or not zone_limits:
            self.blocked_until = datetime.now() + timedelta(seconds=5)

        request_timestamp = int(datetime.now().timestamp() * 1000)
        keys = []
        if server_limits:
            keys.append(self.key_server)
        if zone_limits:
            keys.append(self.key_zone)
        response = 0
        if keys:
            response = await self.redis.evalsha(
                self.permit, len(keys), *keys, request_timestamp
            )

        if int(response) > 0:
            raise LimitBlocked(retry_after=response)
        async with session.get(url) as response:
            response_json = await response.json()
            status = response.status
            headers = response.headers

        response_stamp = int(datetime.now().timestamp() * 1000)

        if "X-App-Rate-Limit" in headers:
            self.logging.debug("Limits for App: %s", server_limits)
            init, update, drop, update_c = await self.response(
                local_limits=server_limits,
                key=self.key_server,
                header_limits=headers.get("X-App-Rate-Limit"),
                header_counts=headers.get("X-App-Rate-Limit-Count"),
                request_timestamp=request_timestamp,
            )
            async with self.redis.pipeline(transaction=True) as pipe:
                if init:
                    pipe.evalsha(
                        self.limits_init, 1, self.key_server, response_stamp, *init
                    )
                if update:
                    pipe.evalsha(self.limits_update, 1, self.key_server, *update)
                if drop:
                    pipe.evalsha(self.limits_drop, 1, self.key_server, *drop)
                for entry in update_c:
                    pipe.evalsha(*entry)
                await pipe.execute()

        if "X-Method-Rate-Limit" in headers:
            self.logging.debug("Limits for Method: %s", zone_limits)
            init, update, drop, update_c = await self.response(
                local_limits=zone_limits,
                key=self.key_zone,
                header_limits=headers.get("X-Method-Rate-Limit"),
                header_counts=headers.get("X-Method-Rate-Limit-Count"),
                request_timestamp=request_timestamp,
            )
            async with self.redis.pipeline(transaction=True) as pipe:
                if init:
                    pipe.evalsha(
                        self.limits_init, 1, self.key_zone, response_stamp, *init
                    )
                if update:
                    pipe.evalsha(self.limits_update, 1, self.key_zone, *update)
                if drop:
                    pipe.evalsha(self.limits_drop, 1, self.key_zone, *drop)
                for entry in update_c:
                    pipe.evalsha(*entry)
                await pipe.execute()

        if status == 200:
            return response_json
        if status == 404:
            raise NotFoundException()
        if status == 429:
            self.logging.debug(headers)
            raise RatelimitException(retry_after=headers.get("Retry-After", 1))
        raise Non200Exception()

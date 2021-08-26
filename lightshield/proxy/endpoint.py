import logging
from datetime import datetime

from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)


class Endpoint:
    def __init__(self, server, zone, namespace, redis):
        self.server = server
        self.namespace = namespace
        self.zone = zone
        self.redis = redis
        self.server_key = "%s:%s" % (
            namespace,
            server,
        )  # Used to identify existence of server wide limits
        self.zone_key = "%s:%s:%s" % (
            namespace,
            server,
            zone,
        )  # Used to identify existence of zone wide limits
        self.server_limits = None
        self.zone_limits = None
        self.logging = logging.getLogger("Proxy")

    async def set_limits(self, key):
        """Execute while running the first request.

        Set a pseudo limit that will block all but a single request until that first request returns.
        This allows generation of the actual rate limits through the response data.
        """
        try:
            pseudo_sha1 = self.pseudo_sha1
        except AttributeError:
            pseudo_sha1 = self.pseudo_sha1 = await self.redis.get("lightshield_pseudo")

        if await self.redis.setnx(key, "1:57") == 0:
            limits = [
                [int(x) for x in limit.split(":")]
                for limit in (await self.redis.get(key)).split(",")
            ]
        else:
            limits = [[1, 57]]
            if await self.redis.evalsha(pseudo_sha1, [key + ":%s" % 57]) == 1:
                raise LimitBlocked(retry_after=1000)
        return limits

    async def permit(self, start_point) -> None:
        """Handle pre-flight ratelimiting unlock.

        Data passed to the script:
            KEYS = [list of bucket keys]
            ARGV = [send timestamp, list of bucket max, end_timestamp for bucket length]
        Will attempt to load the sha1 value from local values. If fails the value is pulled from redis
        Script returns:
            negative value for no wait, 0+ for wait
        """
        try:
            script_sha1 = self.permit_sha1
        except AttributeError:
            script_sha1 = self.permit_sha1 = await self.redis.get("lightshield_permit")
        keys = []
        argv = [start_point]

        if not self.server_limits:
            self.server_limits = await self.set_limits(self.server_key)

        for max, key in self.server_limits:
            keys += [self.server_key + ":%s" % key]
            argv += [max, start_point + key * 1000]

        if not self.zone_limits:  # Zone limits can be missing on their own
            self.zone_limits = await self.set_limits(self.zone_key)

        for max, key in self.zone_limits:
            keys += [self.zone_key + ":%s" % key]
            argv += [max, start_point + key * 1000]

        if (wait := await self.redis.evalsha(script_sha1, keys, argv)) > 0:
            self.logging.debug("Waiting for %s ms.", wait)
            raise LimitBlocked(retry_after=wait)

    async def update_ratelimiter(self, key, limits):
        """Update the saved rate limit buckets.

        Set the intervals that are being looked out for in the rate limiter based on the returned
        'X-App-Rate-Limit' header.
        """
        await self.redis.set(key, limits)

    async def align(self, start_point, headers, status) -> None:
        """Align limits with response details.

        Data passed to the script:
            KEYS = [list of bucket keys]
            ARGV = [send timestamp, return_timestamp, retry_after] + [count, max, span] for each bucket
        Will attempt to load the sha1 value from local values. If fails the value is pulled from redis
        Script returns:
            nothing

        """
        return_point = int(datetime.now().timestamp() * 1000)
        retry_after = 0
        if status != 200 and "Retry-After" in headers:
            retry_after = headers["Retry-After"]

        try:
            script_sha1 = self.align_sha1
        except AttributeError:
            script_sha1 = self.align_sha1 = await self.redis.get("lightshield_update")

        # Update rate limits applied to this server and zone
        await self.update_ratelimiter(self.server_key, headers["X-App-Rate-Limit"])
        await self.update_ratelimiter(self.zone_key, headers["X-Method-Rate-Limit"])

        keys = []
        argv = [start_point, return_point, retry_after]

        self.server_limits = [
            [int(x) for x in limit.split(":")]
            for limit in headers["X-App-Rate-Limit"].split(",")
        ]
        app_counts = [
            [int(x) for x in limit.split(":")]
            for limit in headers["X-App-Rate-Limit-Count"].split(",")
        ]
        app_zipped = zip(  # Zipped version of Count:Span
            [entry[0] for entry in app_counts],
            [entry[1] for entry in self.server_limits],
        )
        for limit in app_zipped:
            argv += limit[:2]
            keys.append(self.server_key + ":%s" % limit[1])

        self.zone_limits = [
            [int(x) for x in limit.split(":")]
            for limit in headers["X-Method-Rate-Limit"].split(",")
        ]
        zone_counts = [
            [int(x) for x in limit.split(":")]
            for limit in headers["X-Method-Rate-Limit-Count"].split(",")
        ]
        zone_zipped = zip(  # Zipped version of Count:Span
            [entry[0] for entry in zone_counts],
            [entry[1] for entry in self.zone_limits],
        )
        for limit in zone_zipped:
            argv += limit
            keys.append(self.zone_key + ":%s" % limit[1])

        await self.redis.evalsha(script_sha1, keys, argv)

    async def request(self, url, session) -> dict:
        """Process request."""
        start_point = int(datetime.now().timestamp() * 1000)
        await self.permit(start_point)

        async with session.get(url) as response:
            data = await response.json()
            headers = response.headers
            status = response.status
            if status != 200:
                self.logging.debug(status)
                self.logging.debug(headers)

        await self.align(start_point, headers, status)
        if status == 200:
            return data
        elif status == 404:
            raise NotFoundException
        elif status == 429:
            raise RatelimitException
        else:
            raise Non200Exception

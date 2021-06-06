from datetime import datetime
from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
    NoMessageException,
)
import logging
import os


class Endpoint:
    def __init__(self, server, zone, redis):
        self.server = server
        self.zone = zone
        self.redis = redis
        self.server_key = (
            "%s" % server
        )  # Used to identify existence of server wide limits
        self.zone_key = "%s:%s" % (
            server,
            zone,
        )  # Used to identify existence of zone wide limits
        self.server_limits = None
        self.zone_limits = None
        self.logging = logging.getLogger("Proxy")

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
            script_sha1 = self.permit_sha1 = await self.redis.get("permit")
        try:
            pseudo_sha1 = self.pseudo_sha1
        except AttributeError:
            pseudo_sha1 = self.pseudo_sha1 = await self.redis.get("pseudo")

        keys = []
        argv = [start_point]

        if (
            not self.server_limits
        ):  # TODO: Updating limits only happens through responses
            # print("No local server limits found")
            if await self.redis.setnx(self.server_key, "1:57") == 0:
                self.server_limits = [
                    [int(x) for x in limit.split(":")]
                    for limit in (await self.redis.get(self.server_key)).split(",")
                ]
                # print("Found server side limits: %s" % self.server_limits)
            else:
                self.server_limits = [[1, 57]]
                # print("No server limits found. Setting to: %s" % self.server_limits)
                if (
                    await self.redis.evalsha(
                        pseudo_sha1, [self.server_key + ":%s" % 57]
                    )
                    == 1
                ):
                    raise LimitBlocked(1000)

        for max, key in self.server_limits:
            keys += [self.server_key + ":%s" % key]
            argv += [max, start_point + key * 1000]

        if not self.zone_limits:  # Zone limits can be missing on their own
            if await self.redis.setnx(self.zone_key, "1:57") == 0:
                self.zone_limits = [
                    [int(x) for x in limit.split(":")]
                    for limit in (await self.redis.get(self.zone_key)).split(",")
                ]
            else:
                self.zone_limits = [(1, 57)]
                if (
                    await self.redis.evalsha(pseudo_sha1, [self.zone_key + ":%s" % 57])
                    == 1
                ):
                    raise LimitBlocked(1000)

        for max, key in self.zone_limits:
            keys += [self.zone_key + ":%s" % key]
            argv += [max, start_point + key * 1000]

        # print("Sending keys: %s" % keys)
        # print("Sending argv: %s" % argv)
        if (wait := await self.redis.evalsha(script_sha1, keys, argv)) > 0:
            self.logging.debug("Waiting for %s ms.", wait)
            raise LimitBlocked(retry_after=wait)

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
            script_sha1 = self.align_sha1 = await self.redis.get("update")

        keys = []
        argv = [start_point, return_point, retry_after]

        # App Limits
        await self.redis.set(self.server_key, headers["X-App-Rate-Limit"])
        self.server_limits = [
            [int(x) for x in limit.split(":")]
            for limit in headers["X-App-Rate-Limit"].split(",")
        ]
        counts = [
            [int(x) for x in limit.split(":")]
            for limit in headers["X-App-Rate-Limit-Count"].split(",")
        ]
        # print(counts)
        # print(self.server_limits)
        zipped = zip(  # Zipped version of Count:Span
            [entry[0] for entry in counts],
            [entry[1] for entry in self.server_limits],
        )
        for limit in zipped:
            argv += limit[:2]
            keys.append(self.server_key + ":%s" % limit[1])

        # App Limits
        await self.redis.set(self.zone_key, headers["X-Method-Rate-Limit"])
        self.zone_limits = [
            [int(x) for x in limit.split(":")]
            for limit in headers["X-Method-Rate-Limit"].split(",")
        ]
        counts = [
            [int(x) for x in limit.split(":")]
            for limit in headers["X-Method-Rate-Limit-Count"].split(",")
        ]
        # print(counts)
        # print(self.zone_limits)
        zipped = zip(  # Zipped version of Count:Span
            [entry[0] for entry in counts],
            [entry[1] for entry in self.zone_limits],
        )
        for limit in zipped:
            argv += limit
            keys.append(self.zone_key + ":%s" % limit[1])

        # print(keys)
        # print(argv)
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

from lightshield.exceptions import (
    RatelimitException,
    NotFoundException,
    Non200Exception,
    LimitBlocked
)


class Middleware:
    key = None
    header = None

    async def align(self, data):
        """Pseudo method for outwards processing."""

        await self.update_ratelimiter(data.redis, data.response_headers[self.header + "-Rate-Limit"])

        limits_tupled = [
            [int(x) for x in limit.split(":")]
            for limit in data.response_headers[self.header + "-Rate-Limit"].split(",")
        ]
        counts = [
            [int(x) for x in limit.split(":")]
            for limit in data.response_headers[self.header + "-Rate-Limit-Count"].split(",")
        ]
        zipped = zip(  # Zipped version of Count:Span
            [entry[0] for entry in counts],
            [entry[1] for entry in limits_tupled],
        )
        for limit in zipped:
            data.a_argv += limit[:2]
            data.a_keys.append(self.key + ":%s" % limit[1])

    async def middleware(self, data):
        """Default method for middleware processing."""
        # Inward collecting args
        if not self.limits:
            self.limits = await self.set_limits(data.redis, self.key)

        try:
            await next(data.gen)(data)
            await self.align(data)
        except LimitBlocked as err:
            raise err
        except (RatelimitException, NotFoundException, Non200Exception) as err:
            await self.align(data)
            raise err

    async def update_ratelimiter(self, redis, limits):
        """Update the saved rate limit buckets.

        Set the intervals that are being looked out for in the rate limiter based on the returned
        'X-App-Rate-Limit' header.
        """
        if self.limits != limits:
            await redis.set(self.key, limits)

    async def set_limits(self, redis, key):
        """Execute while running the first request.

        Set a pseudo limit that will block all but a single request until that first request returns.
        This allows generation of the actual rate limits through the response data.
        """
        try:
            pseudo_sha1 = self.pseudo_sha1
        except AttributeError:
            pseudo_sha1 = self.pseudo_sha1 = await redis.get("lightshield_pseudo")

        if await redis.setnx(key, "1:57") == 0:
            limits = [
                [int(x) for x in limit.split(":")]
                for limit in (await redis.get(key)).split(",")
            ]
        else:
            limits = [[1, 57]]
            if await redis.evalsha(pseudo_sha1, [key + ":%s" % 57]) == 1:
                raise LimitBlocked(retry_after=1000)
        return limits

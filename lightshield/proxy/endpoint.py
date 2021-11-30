import logging
import types
from datetime import datetime

from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)


class Endpoint:
    """Handle requests for a specific endpoint."""

    def __init__(self, server, zone, redis, middlewares):
        self.server = server
        self.zone = zone
        self.middlewares = middlewares
        self.redis = redis
        self.logging = logging.getLogger("Proxy")

    def _gen(self):
        """Generator method to return all methods."""
        list = [self._align] + [mw.middleware for mw in self.middlewares] + [self._permit, self._request]

        for entry in list:
            yield entry

    async def _permit(self, data):
        """Get permit for request execution."""
        try:
            script_sha1 = self.permit_sha1
        except AttributeError:
            script_sha1 = self.permit_sha1 = await self.redis.get("lightshield_permit")

        if (wait := await self.redis.evalsha(script_sha1, data.keys, data.argv)) > 0:
            self.logging.debug("Waiting for %s ms.", wait)
            raise LimitBlocked(retry_after=wait)

        return await next(data.gen)(data)  # Nothing to execute on the back

    async def _align(self, data):
        """Align limits based on headers."""
        try:
            script_sha1 = self.align_sha1
        except AttributeError:
            script_sha1 = self.align_sha1 = await self.redis.get("lightshield_update")

        await next(data.gen)(data)
        await self.redis.evalsha(script_sha1, data.a_keys, data.a_argv)

    async def _request(self, data):
        """Execute the query against the API.

        At this point permits are set and the return is passed back through the layers.
        """
        async with data.session.get(data.url) as response:
            data.response_json = await response.json()
            data.response_headers = response.headers
            status = response.status
            headers = response.headers
            # Prefill a_argv
            retry_after = 0
            if status != 200 and "Retry-After" in headers:
                retry_after = headers["Retry-After"]
            data.a_argv += [int(datetime.now().timestamp() * 1000), retry_after]

            if status == 200:
                return
            if status == 404:
                raise NotFoundException()
            if status == 429:
                raise RatelimitException()
            raise Non200Exception()

    async def request(self, url, session):
        """Handle requests."""
        data = types.SimpleNamespace()
        # Static
        data.session = session
        data.url = url
        data.redis = self.redis
        # Permit
        data.keys = []
        data.argv = [int(datetime.now().timestamp() * 1000)]
        # Algin
        data.a_keys = []
        data.a_argv = data.argv
        # Generator Method
        data.gen = self._gen()
        await next(data.gen)(data)
        return data.response_json

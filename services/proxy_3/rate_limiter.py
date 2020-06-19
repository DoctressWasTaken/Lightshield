import asyncio
from aiohttp.web import middleware, HTTPException
import os
import json

from limiter import LimitBlocked, LimitHandler


class HTTPTooManyRequestsLocal(HTTPException):
    """Local 429 Exception. Separated from the server side exception for logging purposes."""
    status_code = 430


class AppLimiter:
    """Middleware that checks app wide limits."""

    def __init__(self):
        self.required_header = ['Date', 'X-App-Rate-Limit-Count']
        limits = json.loads(open("limits.json", "r").read())
        self.limits = [LimitHandler(limit.split(":")) for limit in limits['APP']]

    @middleware
    async def middleware(self, request, handler):
        """Process the request.

        request: Add X-Riot-Token Header with the API Key.
        response: No changes.
        """
        for limit in self.limits:
            try:
               limit.add
            except LimitBlocked as err:
                raise HTTPTooManyRequestsLocal(headers={"Retry-After": str(err.retry_after)})
        response = await handler(request)
        if 'X-App-Rate-Limit-Count' not in response.headers:
            raise HTTPException
        if 'Date' not in response.headers:
            raise HTTPException
        for limit in self.limits:
            await limit.update(
                response.headers['Date'],
                response.headers['X-App-Rate-Limit-Count'].split(","))
        return response


class MethodLimiter:
    """Middleware that checks method wide limits."""

    def __init__(self):
        self.required_header = ['Date', 'X-Method-Rate-Limit-Count']
        limits = json.loads(open("limits.json", "r").read())['METHODS']
        self.limits = {}
        for method in limits:
            self.limits[method] = LimitHandler(limits[method].split(":"))

    @middleware
    async def middleware(self, request, handler):
        """Process the request.

        request: Add X-Riot-Token Header with the API Key.
        response: No changes.
        """
        url = str(request.url)
        method = url.split("/lol/")[1].split("/", 1)[0]

        try:
            self.limits[method].add
        except LimitBlocked as err:
            raise HTTPTooManyRequestsLocal(headers={"Retry-After": str(err.retry_after)})
        response = await handler(request)
        if 'X-Method-Rate-Limit-Count' not in response.headers:
            raise HTTPException
        if 'Date' not in response.headers:
            raise HTTPException
        await self.limits[method].update(
                response.headers['Date'],
                response.headers['X-Method-Rate-Limit-Count'].split(","))
        return response

# -*- coding: utf-8 -*-
"""Proxy Module.

Routes and ratelimits all calls to the API.
"""
import asyncio  # noqa: F401
from aiohttp import web
from aiohttp.web import HTTPInternalServerError
import aiohttp
import sys
import os
sys.path.append(os.getcwd())

from datetime import timezone, datetime, timedelta
import pytz
import json
import logging

# Middleware
from auth import Headers, Logging
from rate_limiter import AppLimiter, MethodLimiter

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

MIDDLEWARES = [
    AppLimiter,
    MethodLimiter,
    Headers,
    Logging
]

class Proxy:
    """The proxy class contains the proxy server to be run.

    Creates an API Handler object for rate limiting.
    run() to start the webserver.
    """

    def __init__(self):
        """Set external objects and routes."""

        self.middlewares = [cls() for cls in MIDDLEWARES]
        self.required_header = []
        for middleware in self.middlewares:
            self.required_header += middleware.required_header

        self.app = web.Application(middlewares=[cls.middleware for cls in self.middlewares])
        self.app.add_routes([
            web.get('/{tail:.*}', self.request)

        ])

    def run(self, host="0.0.0.0", port=8080):
        """Run Method.

        Called externally to start the proxy webserver.
        """
        web.run_app(self.app, port=port, host=host)

    async def request(self, request):
        """Pass message."""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2.5)) as session:
                async with session.get(request.url, headers=dict(request.headers)) as response:
                    body = await response.json()
        except Exception as err:
            print("Got", err)
            raise HTTPInternalServerError()

        headers = dict(response.headers)
        returned_headers = {}
        for header in headers:
            if header in self.required_header:
                returned_headers[header] = headers[header]
        return web.Response(text=json.dumps(body), headers=returned_headers, status=response.status)


if __name__ == '__main__':
    proxy = Proxy()
    proxy.run(port=8000, host="0.0.0.0")

import asyncio
from aiohttp.web import middleware
import os


class Headers:
    """Middleware that adds the Riot API Key to the request."""
    def __init__(self):
        if not "API_KEY" in os.environ:
            raise Exception("No API Key provided.")
        self.api_key = os.environ['API_KEY']
        self.required_header = []

    @middleware
    async def middleware(self, request, handler):
        """Process the request.

        request: Add X-Riot-Token Header with the API Key.
        response: No changes.
        """
        headers = dict(request.headers)
        headers.update({'X-Riot-Token': self.api_key})
        url = str(request.url)
        request = request.clone(headers=headers, rel_url=url.replace("http:", "https:"))
        return await handler(request)

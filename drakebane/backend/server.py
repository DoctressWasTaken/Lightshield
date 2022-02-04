import json
import asyncio
import aiohttp_cors
import aioredis
from aiohttp import web


class Server:
    def __init__(self):
        """Load settings from file and initiate the web server."""
        try:
            with open("settings.json", "r") as settings:
                self.settings = json.loads(settings.read())
        except FileNotFoundError:
            with open("settings_default.json", "r") as settings:
                self.settings = json.loads(settings.read())
            with open("settings.json", "w+") as settings:
                json.dump(self.settings, settings, indent=2, sort_keys=True)
        asyncio.run(self.update_settings())
        self.app = web.Application()
        cors = aiohttp_cors.setup(
            self.app,
            defaults={
                "*": aiohttp_cors.ResourceOptions(),
            },
        )
        all_cors = cors.add(self.app.router.add_resource("/config"))
        cors.add(all_cors.add_route("POST", self.settings_set))
        cors.add(all_cors.add_route("GET", self.settings_get))

    async def settings_get(self, request):
        """Return the current local settings."""
        return web.Response(text=json.dumps(self.settings))

    async def settings_set(self, request):
        """Get updated settings from web interface and write them to file/update in memory."""
        self.settings = await request.json()
        with open("settings.json", "w+") as settings:
            json.dump(self.settings, settings, indent=2, sort_keys=True)
        await self.update_settings()
        return web.Response(text=json.dumps({"result": "done"}))

    async def update_settings(self):
        con = await aioredis.create_redis("redis://redis:6379")
        await con.set("regions", json.dumps(self.settings["regions"]))
        await con.set("apiKey", self.settings["apiKey"])

        formatted = {}
        for key, value in self.settings["services"].items():
            if value:
                await con.set('service_%s' % key, 'true')
            else:
                await con.set('service_%s' % key, 'false')

    def run(self):
        web.run_app(self.app, port=8302)

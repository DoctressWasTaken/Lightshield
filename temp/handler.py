import asyncio
from aiohttp import web

class Request:

    def __init__(self):
        self.done = False


    async def run(self):

        await asyncio.sleep(10)
        print("done")
        return
        for i in range(800):
            await asyncio.sleep(0.01)




class Handler:

    def __init__(self):

        self.calls = 0

    async def handle(self, request):

        self.calls += 1
        print("Current calls: ", self.calls)
        request_objects = []
        requests = []
        for i in range(50):
            r = Request()
            request_objects.append(r)
            requests.append(asyncio.create_task(r.run()))
        await asyncio.gather(*requests)
        self.calls -= 1
        print("Current calls: ", self.calls)
        return web.Response(text="Mock")

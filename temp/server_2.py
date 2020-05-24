#!/usr/bin/env python3.5

from aiohttp import web
import asyncio
import random
import json
import concurrent.futures
import time
import datetime
import threading

tasks = []

dat = json.dumps({"status": "Hello World"})

ended = False

def worker(loop):
    asyncio.set_event_loop(loop)
    asyncio.run(method())

async def method():
    global ended
    while not ended:
        if not tasks:
            print("Waiting")
            await asyncio.sleep(1)
        else:
            request = tasks.pop()
            return web.Response(text="Mock")
    print("Ended")


async def handle(request):

    start = datetime.datetime.now()
    print(start)
    tasks.append(request)
    #with concurrent.futures.ThreadPoolExecutor() as pool:
    #    res = await loop.run_in_executor(pool, fetch)
    #print(request, (datetime.datetime.now() - start).total_seconds())
    #while True:
    #    await asyncio.sleep(100)
    await asyncio.sleep(1000)
    return web.Response(text="Real")

async def init():
    print("Initiated")
    app = web.Application()
    app.router.add_route('GET', '/{name}', handle)
    return await loop.create_server(
            app.make_handler(), '127.0.0.1', 8888)


loop = asyncio.get_event_loop()

new_loop = asyncio.new_event_loop()
t = threading.Thread(target=worker, args=(new_loop,))
t.start()
try:
    loop.run_until_complete(init())
    loop.run_forever()
except KeyboardInterrupt as err:
    print("Interrupted")
    ended = True
    t.join()

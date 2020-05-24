# -*- coding: utf-8 -*-
"""Proxy Module.

Routes and ratelimits all calls to the API.
"""
import asyncio  # noqa: F401
from aiohttp import web
import aiohttp
import threading

from datetime import timezone, datetime, timedelta
import pytz
import os
import json
import logging
import time
from _app import AppApiHandler
from endpoint_thread import Endpoint
from _websocket import Websocket
import websockets
import signal

logging.basicConfig(filename='foo', level=logging.DEBUG)
root_logger = logging.getLogger()
root_logger.handlers = []

# Temporal addition
if "SERVER" not in os.environ:
    logging.info("No SERVER env variable provided. Exiting.")
    exit()
if "API_KEY" not in os.environ:
    logging.info("No API_KEY env variable provided. Exiting.")
    exit()

server = os.environ["SERVER"]
headers = {'X-Riot-Token': os.environ['API_KEY']}

limits = json.loads(open("limits.json", "r").read())

base_url = f"https://{server}.api.riotgames.com/lol"
count = 0

def shutdown(signal, stack):
    print("Closing")
    websocket.stop()
    for endpoint in endpoint_threads:
        endpoint.stop()

    for endpoint in endpoint_threads:
        endpoint.join()

if __name__ == '__main__':

    app = AppApiHandler(limits['APP'])
    endpoint_threads = []
    queues = {}
    return_dict = {}
    default_loop = asyncio.get_event_loop()
    for endpoint_name in limits['METHODS']:
        loop = asyncio.new_event_loop()
        endpoint = Endpoint(
            endpoint_name,
            limits['METHODS'][endpoint_name],
            app,
            loop,
            return_dict,
            headers)
        queues[endpoint_name] = endpoint.init_queue()
        endpoint_threads.append(endpoint)
        endpoint.start()
    websocket = Websocket(queues, return_dict, base_url, host="0.0.0.0")
    print(queues)
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    asyncio.set_event_loop(default_loop)
    asyncio.run(websocket.serve())


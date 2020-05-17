"""Match History updater. Pulls matchlists for all player."""

import asyncio
import uvloop
import signal
import aiohttp
import json
import os
import time
import pika
from aiohttp.client_exceptions import ClientConnectorError

if "SERVER" not in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

os.environ['STATUS'] = "RUN"
# matchId
base_url = f"http://proxy:8000/match/v4/matches/%s"

from worker import Worker

def end_handler(sig, frame):

    os.environ['STATUS'] = "STOP"


async def main():
    """Update user match lists.

    Wrapper function that starts the cycle.
    Pulls data from the DB in syncronous setup,
    calls requests in async method and uses the returned values to update.
    """
    # Pull data package
    w = Worker()
    await w.connect_rabbit()
    await w.run()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, end_handler)

    uvloop.install()
    asyncio.run(main())

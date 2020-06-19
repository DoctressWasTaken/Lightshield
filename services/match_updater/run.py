"""Match History updater. Pulls matchlists for all player."""

import asyncio
import uvloop
import signal
import os

if "SERVER" not in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

os.environ['STATUS'] = "RUN"

from worker import Master

def end_handler(sig, frame):

    os.environ['STATUS'] = "STOP"


async def main():
    """Update user match lists.

    Wrapper function that starts the cycle.
    Pulls data from the DB in syncronous setup,
    calls requests in async method and uses the returned values to update.
    """
    # Pull data package
    m = Master(buffer=50)
    await m.run()



if __name__ == "__main__":
    signal.signal(signal.SIGTERM, end_handler)

    uvloop.install()
    asyncio.run(main())

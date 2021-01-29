import patch_updater
import asyncio
import datetime
import os
from permanent_db import PermanentDB
import time
import threading
import signal

from match_processor import MatchProcessor
from summoner_processor import SummonerProcessor


def main():

    permanent = PermanentDB()
    server = os.environ['SERVER']
    patches = asyncio.run(patch_updater.get())

    offsets = {"KR": -39600,
               "EUW1": -10800,
               "NA1": 10800}

    matchP = MatchProcessor(server, offsets[server], patches, permanent)
    summonerP = SummonerProcessor(server, offsets[server], patches, permanent)

    stopped = False
    def shutdown_handler():
        global stopped
        stopped = True
        matchP.shutdown()
        summonerP.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    matchP.run()
    summonerP.run()

    while not stopped:
        patches = asyncio.run(patch_updater.get())
        matchP.update_patches(patches)
        summonerP.update_patches(patches)
        for i in range(200):
            time.sleep(3)
            if stopped:
                break

    matchP.join()
    summonerP.join()


async def main():
    permanent = PermanentDB()
    server = os.environ['SERVER']

    #matchP = MatchProcessor(server, offsets[server], patches, permanent)
    summonerP = SummonerProcessor(server, permanent)
    summonerTask = asyncio.create_task(summonerP.run())

    def shutdown_handler():
        summonerP.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    await summonerTask

if __name__ == "__main__":
    asyncio.run(main())


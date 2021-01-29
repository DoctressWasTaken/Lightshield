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


async def main():
    permanent = PermanentDB()
    server = os.environ['SERVER']

    summonerP = SummonerProcessor(server, permanent)

    summonerTask = asyncio.create_task(summonerP.run())
   

    def shutdown_handler():
        summonerP.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    await summonerTask

if __name__ == "__main__":
    asyncio.run(main())


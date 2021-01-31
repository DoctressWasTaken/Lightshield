import asyncio
import os
import signal

import uvloop
from permanent_db import PermanentDB
from summoner_processor import SummonerProcessor

uvloop.install()


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


import asyncio
import os
import signal

import uvloop
from match_processor import MatchProcessor
from permanent_db import PermanentDB

uvloop.install()


async def main():
    permanent = PermanentDB()
    server = os.environ['SERVER']

    matchP = MatchProcessor(server, permanent)

    matchTask = asyncio.create_task(matchP.run())


    def shutdown_handler():
        matchP.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    await matchTask

if __name__ == "__main__":
    asyncio.run(main())


import asyncio
import datetime
import os
from permanent_db import PermanentDB
import time
import threading
import signal
from match_processor import MatchProcessor


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


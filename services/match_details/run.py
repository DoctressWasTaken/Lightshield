import asyncio
import signal

import uvloop
from logic import Service
from runes import get_ids

uvloop.install()

if __name__ == "__main__":
    service = Service(get_ids())

    def shutdown_handler():
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    asyncio.run(service.run())

import asyncio
import logging
import os
import signal

import uvloop

from logic import Service

uvloop.install()
if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )

if __name__ == "__main__":
    service = Service()

    def shutdown_handler():
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    asyncio.run(service.run())

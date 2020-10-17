from logic import Service

import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from rabbit_sync import RabbitManager

if __name__ == "__main__":

    rabbit = RabbitManager(
        exchange="SUMMONER",
        incoming="RANKED_TO_SUMMONER",
        outgoing=['SUMMONER_TO_HISTORY', 'SUMMONER_TO_PROCESSOR']
    )

    service = Service()

    def shutdown_handler():
        """Shutdown."""
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    asyncio.run(service.run())

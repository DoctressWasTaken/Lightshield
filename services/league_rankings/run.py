from publisher import Publisher
from logic import Service

import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == "__main__":
    asyncio.run(marker.build(
           "CREATE TABLE IF NOT EXISTS match_history("
           "summonerId TEXT PRIMARY KEY,"
           "matches INTEGER);"))

    publisher = Publisher()
    service = Service()

    def shutdown_handler():
        publisher.shutdown()
        service.shutdown()
    signal.signal(signal.SIGTERM, shutdown_handler)

    publisher.start()
    asyncio.run(service.run())

    publisher.join()

from logic import Service
import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from rabbit_sync import RabbitManager

if __name__ == "__main__":

    service = Service()

    rabbit = RabbitManager(
        exchange="RANKED",
        outgoing=['RANKED_TO_SUMMONER']
    )


    def shutdown_handler():
        service.shutdown()
    signal.signal(signal.SIGTERM, shutdown_handler)

    rabbit.start()
    asyncio.run(service.run(rabbit))

    rabbit.join()

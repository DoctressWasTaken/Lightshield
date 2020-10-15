from logic import Service
from rabbit_manager import Manager
import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == "__main__":

    service = Service()

    def shutdown_handler():
        rabbitmq.shutdown()
        service.shutdown()
    signal.signal(signal.SIGTERM, shutdown_handler)

    rabbitmq.start()
    asyncio.run(service.run())

    rabbitmq.join()

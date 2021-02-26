import asyncio
import signal

from logic import Service

# uvloop.install()

if __name__ == "__main__":
    service = Service()

    def shutdown_handler():
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    asyncio.run(service.run())
    service.rabbit.shutdown()

import signal
import asyncio
from websocket_worker import Worker
import docker
import time
import yaml
import os
import json
from container import Container

config = json.loads(open("config.json").read())

os.environ['API_KEY'] = json.loads(open("secrets.json").read())['API_KEY']

from server import ServerManager


def end_handler(sig, frame):
    """Translate the docker stop signal SIGTERM to KeyboardInterrupt."""
    raise KeyboardInterrupt


def main():
    # Startup Process - General
    client = docker.from_env()
    postgres = Container(
        client,
        'riotapi_postgres',
        **config["container"]['riotapi_postgres']
    )
    postgres.startup(wait_for="0.0.0.0:5432")
    rabbitmq = Container(
        client,
        'riotapi_rabbitmq',
        **config["container"]['riotapi_rabbitmq'])
    rabbitmq.startup(wait_for="0.0.0.0:5672")

    # Startup Process - Depending on Config
    server_threads = {}
    for server in config['server']:
        print(server, "\t", config['server'][server])
        if config['server'][server]:
            server_threads[server] = ServerManager(server, client, rabbitmq,
                                                   postgres, config)
            server_threads[server].startup()
            server_threads[server].start()

    # Permanent loop
    try:
        signal.signal(signal.SIGTERM, end_handler)
        w = Worker(server_threads, rabbitmq, postgres)
        asyncio.run(w.start())

    except KeyboardInterrupt:
        print("Received Keyboard interrupt")
        for thread in server_threads:
            server_threads[thread].stop()
    for thread in server_threads:
        server_threads[thread].join()


if __name__ == "__main__":
    main()

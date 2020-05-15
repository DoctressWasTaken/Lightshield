import signal

import docker
import time
import yaml
import os
import json
from container import Container

config = json.loads(open("config.json").read())

api_key = json.loads(open("secrets.json").read())['API_KEY']

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
        **config["server"]['riotapi_postgres']
    )
    postgres.startup(wait_for="0.0.0.0:5432")
    rabbitmq = Container(
        client,
        'riotapi_rabbitmq',
        **config["server"]['riotapi_rabbitmq'])
    rabbitmq.startup(wait_for="0.0.0.0:5672")

    # Startup Process - Depending on Config
    euw = ServerManager("EUW1", client, rabbitmq, api_key)
    euw.start()

    # Permanent loop
    try:
        signal.signal(signal.SIGTERM, end_handler)
        while True:
            print("running")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Received Keyboard interrupt")
        euw.stop()
    euw.join()


if __name__ == "__main__":
    main()

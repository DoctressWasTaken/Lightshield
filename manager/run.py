import docker
import time
import yaml
import os

class Watchdog:

    def __init__(self):
        self.client = docker.from_env()
        with open('container.yaml', 'r') as stream:
            self.container = yaml.safe_load(stream)


    def run(self):
        pass

    def startup(self):
        services = self.container['services']
        started = []
        del services['manager']
        to_start = services.keys()
        print(to_start)
        c = self.client.containers.run(
            'riotapi_db_worker',
            detach=True,
            remove=False,
            name="EUW1_db_worker",
            environment={'SERVER': 'EUW1'})
        print(c)
        for i in range(10):
            print(c.status)
            time.sleep(5)
        c.stop()


def main():

    # Init Watchdog
    w = Watchdog()
    w.startup()
    # init rabbitmq tracking
    time.sleep(15)
    # init db tracking


if __name__ == "__main__":
    main()
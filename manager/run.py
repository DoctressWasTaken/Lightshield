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

    def build(self, build, name):
        print(build)
        path = os.path.join(
            "sources",
            build['context'],
            ""
        )
        print(path)
        return self.client.images.build(
            path=path,
            tag=name)

    def startup(self):
        services = self.container['services']
        started = []
        del services['manager']
        while services:
            for key in services:
                service = services[key]
                if "depends_on" in service:
                    ready = True
                    dependencies = service["depends_on"]
                    for dependency in dependencies:
                        if dependency not in started:
                            ready = False
                            break
                    if  not ready:
                        continue
                if "build" in service:
                    print(self.build(service["build"], key))


def main():

    # Init Watchdog
    w = Watchdog()
    w.startup()
    # init rabbitmq tracking
    time.sleep(15)
    # init db tracking


if __name__ == "__main__":
    main()
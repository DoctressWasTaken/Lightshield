import time
import docker
import logging

class Container:

    def __init__(self,
                 dockerclient,
                 image,
                 server: str = None,
                 api_key: str = None,
                 **kwargs):
        """Init a container object.

        Required param dockerclient object.
        Further optional params are passed to the docker run method.
        """
        self.client = dockerclient
        self.kwargs = kwargs
        self.container = None
        self.image = image
        self.paused = False
        self.logging = logging.getLogger(self.image)
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [{self.image}] %(message)s'))
        self.logging.addHandler(ch)


        if "environment" not in self.kwargs:
            self.kwargs['environment'] = {}
        if api_key:
            self.kwargs['environment']['API_KEY'] = api_key
        if server:
            if "volumes" in self.kwargs:
                self.kwargs['volumes'] = [vol.replace('{server}', server) for
                                          vol in self.kwargs['volumes']]
            if "links" in self.kwargs:
                temp = {}
                for key in self.kwargs['links']:
                    temp[key.replace("{server}", server)] = \
                        self.kwargs['links'][key]
                self.kwargs['links'] = temp

            self.kwargs['environment']["SERVER"] = server
            self.kwargs['name'] = server + "_" + self.kwargs['name']

    def cmd(self, command):
        return self.container.exec_run(command)

    def wait_for(self, target):
        """Use a waiting script to await the service being reachable."""
        cmd = '/project/wait-for-it.sh -q ' + target + ' -- echo "UP"'
        self.logging.info(cmd)
        res = self.container.exec_run(cmd)
        if res.output.decode("utf-8").strip() == "UP":
            return
        self.logging.info(res)
        raise Exception("The Container was not properly started.")

    def check_status(self):
        """Check if the container is already up."""
        try:
            self.container = self.client.containers.get(self.kwargs['name'])
            return self.container.status
        except docker.errors.NotFound as err:
            return None

    def startup(self, wait_for: str = None, wait: int = None):

        status = self.check_status()
        if status == "running":
            self.logging.info(f"Container found running.")
            return
        if status == "exited":
            self.logging.info(f"Removed exited. Recreating.")
            self.container.remove()

        if not status or status == "exited":
            self.logging.info(f"Starting")
            self.container = self.client.containers.run(
                self.image,
                detach=True,
                **self.kwargs)
            for i in range(20):
                time.sleep(0.5)
                self.container.reload()
                if self.container.status == "exited":
                    raise Exception
                if self.container.status == "running":
                    if wait_for:
                        self.wait_for(wait_for)
                    if wait:
                        time.sleep(wait)
                    return
            self.logging.info(f"Container could not be started.")

        elif status == "paused":
            return

    def pause(self):
        if not self.paused:
            self.logging.info(f"Stopping")
            self.container.stop()
            self.paused = True

    def unpause(self):
        if self.paused:
            self.logging.info("Restarting")
            self.container.start()
            self.paused = False

    def stop(self):
        try:
            self.container.kill()
        except:
            pass
        while self.container.status != "exited":
            self.container.reload()
            time.sleep(0.5)
        self.container.remove()

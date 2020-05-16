import time
import docker


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
        print(cmd)
        res = self.container.exec_run(cmd)
        if res.output.decode("utf-8").strip() == "UP":
            return
        print(res)
        raise Exception("The Container was not properly started.")

    def check_running(self):
        """Check if the container is already up."""
        try:
            self.container = self.client.containers.get(self.kwargs['name'])
            if self.container.status != "running":
                print("Status of container is", self.container.status)
                raise Exception
            print("Found", self.image, "already running.")
            return True
        except docker.errors.NotFound as err:
            pass
        except docker.errors.APIError as err:
            pass
        return False

    def startup(self, wait_for: str = None, wait: int = None):
        if not self.check_running():
            print("Starting", self.image)
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

    def pause(self):
        if not self.paused:
            print("Pausing", self.kwargs['name'])
            self.container.pause()
            self.paused = True

    def unpause(self):
        if self.paused:
            print("Unpausing", self.kwargs['name'])
            self.container.unpause()
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

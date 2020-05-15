import time
import docker

class Container:

    def __init__(self,
                 dockerclient,
                 image,
                 server: str = None,
                 **kwargs):
        """Init a container object.

        Required param dockerclient object.
        Further optional params are passed to the docker run method.
        """
        self.client = dockerclient
        self.kwargs = kwargs
        self.container = None
        self.image = image

    def wait_for(self, target):
        """Use a waiting script to await the service being reachable.

        TODO: Not working currently
            wait-for-it.sh is not a working way because it does not
            run in container on windows.
        """
        cmd = '/project/wait-for-it.sh -q ' + target + ' -- echo "UP"'
        print(cmd)
        res = self.container.exec_run(cmd)
        if res.output.decode("utf-8").strip() == "UP":
            return
        print(res)
        raise Exception("The Container was not properly started.")

    def startup(self, wait_for: str = None, wait: int = None):
        print("Starting", self.image)
        try:
            container = self.client.containers.get(self.kwargs['name'])
            self.container = container
            if self.container.status != "running":
                print("Status of container is", self.container.status)
                raise Exception
            print("Found", self.image, "already running.")
            return
        except docker.errors.NotFound as err:
            pass
        except docker.errors.APIError as err:
            pass
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


    def stop(self):
        self.container.kill(signal="SIGINT")
        while self.container.status != "exited":
            self.container.reload()
            time.sleep(0.5)
        self.container.remove()
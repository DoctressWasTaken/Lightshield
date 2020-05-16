import threading
import time
import json

from server.service import Services, Proxy
from server.database import Databases

class ServerManager(threading.Thread):

    def __init__(self, server, dockerclient, rabbitmq, postgres, config):
        """Initiate relevant variables."""
        super().__init__()
        self._is_interrupted = False

        self.services = Services(server, dockerclient, rabbitmq, config)
        self.databases = Databases(server, dockerclient, postgres, config)
        self.proxy = Proxy(server, dockerclient, config)

        self.stats = {
            "database_sizes": None
        }

    def __str__(self):
        """Built in method returned when output as string."""
        return f"Server_Manager [{self.server}]"

    def __repr__(self):
        """Built in method returned when output in list."""
        return self.__str__()

    def startup(self):
        """Start all container in order."""
        self.databases.startup()
        self.proxy.startup()
        self.services.startup()

    def _shutdown(self):
        """Shut down all container running."""
        self.services.shutdown()
        self.proxy.shutdown()
        self.databases.shutdown()

    def stop(self):
        """Set the thread to stop gracefully."""
        self._is_interrupted = True

    def run(self):
        """Run method initiating tasks."""

        while not self._is_interrupted:
            self.services.validate_existence()
            self.proxy.get_request_count()
            self.stats['database_sizes'] = self.databases.get_data_size()

        self._shutdown()
        print("Exiting", self.server)

    async def get_stats(self):
        """Return the current stats of the server in async."""
        return self.stats
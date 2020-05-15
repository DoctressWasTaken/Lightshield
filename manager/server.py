import threading
import time

from container import Container


class ServerManager(threading.Thread):

    def __init__(self, server, dockerclient, rabbitmq, api_key):
        """Initiate relevant variables."""
        super().__init__()
        self._is_interrupted = False
        self.server = server
        self.api_key = api_key
        self.client = dockerclient
        self.container = {'rabbitmq': rabbitmq}

    def __str__(self):
        """Built in method returned when output as string."""
        return f"Server_Manager [{self.server}]"

    def __repr__(self):
        """Built in method returned when output in list."""
        return self.__str__()

    def _startup(self):
        """Start all container in order."""
        # initiate the redis databases and proxy
        self.container['proxy'] = Container(
            self.client,
            "riotapi_proxy",
            name=self.server + "_proxy",
            environment={
                "API_KEY": self.api_key,
                "SERVER": self.server})
        self.container['proxy'].startup(wait_for="0.0.0.0:8000")

        self.container['redis_summoner_id'] = Container(
            self.client,
            "riotapi_redis_summoner_id",
            name=self.server + "_redis_summoner_id",
            volumes=["redis_summoner:/data/"])
        self.container['redis_summoner_id'].startup()
        print("Started Redis for Summoner IDs")

        redis_match_history = Container(
            self.client,
            "riotapi_redis_match_history",
            name=self.server + "_redis_match_history",
            volumes=["redis_summoner:/data/"])
        redis_match_history.startup()
        print("Started Redis for Match Histories")

        redis_match = Container(
            self.client,
            "riotapi_redis_match",
            name=self.server + "_redis_match",
            volumes=["redis_summoner:/data/"])
        redis_match.startup()
        print("Started Redis for Matches")
        # initiate all services backwards
        time.sleep(5)

        db_worker = Container(
            self.client,
            "riotapi_db_worker",
            name=self.server + "_db_worker",
            links={
                "riotapi_postgres": "postgres",
                "riotapi_rabbitmq": "rabbitmq"},
            environment={"SERVER": "EUW1"}
        )
        db_worker.startup()
        while not self._is_interrupted:
            print("Waiting")
            time.sleep(1)
        print("Shutting down")


    def stop(self):
        """Set the thread to stop gracefully."""
        self._is_interrupted = True

    def run(self):
        """Run method initiating tasks."""

        self._startup()

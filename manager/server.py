import threading
import time
import json

from container import Container


class ServerManager(threading.Thread):

    def __init__(self, server, dockerclient, rabbitmq, api_key,
                 config):
        """Initiate relevant variables."""
        super().__init__()
        self._is_interrupted = False
        self.server = server
        self.api_key = api_key
        self.client = dockerclient
        self.container_data = config['container']
        self.cutoffs = config['cutoffs']
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
            server=self.server,
            api_key=self.api_key,
            **self.container_data['riotapi_proxy'])
        self.container['proxy'].startup(wait_for="0.0.0.0:8000")

        self.container['redis_summoner_id'] = Container(
            self.client,
            "riotapi_redis_summoner_id",
            server=self.server,
            **self.container_data['riotapi_redis_summoner_id'])
        self.container['redis_summoner_id'].startup()

        self.container['redis_match_history'] = Container(
            self.client,
            "riotapi_redis_match_history",
            server=self.server,
            **self.container_data['riotapi_redis_match_history'])
        self.container['redis_match_history'].startup(wait=1)

        self.container['redis_match'] = Container(
            self.client,
            "riotapi_redis_match",
            server=self.server,
            **self.container_data['riotapi_redis_match'])
        self.container['redis_match'].startup(wait=1)
        # initiate all services backwards
        time.sleep(1)

        self.container['db_worker'] = Container(
            self.client,
            "riotapi_db_worker",
            server=self.server,
            **self.container_data['riotapi_db_worker'])
        self.container['db_worker'].startup(wait=1)

        self.container['match_updater'] = Container(
            self.client,
            "riotapi_match_updater",
            server=self.server,
            **self.container_data["riotapi_match_updater"])
        self.container['match_updater'].startup()

        self.container['match_history_updater'] = Container(
            self.client,
            "riotapi_match_history_updater",
            server=self.server,
            **self.container_data["riotapi_match_history_updater"])
        self.container['match_history_updater'].startup()

        self.container['summoner_id_updater'] = Container(
            self.client,
            "riotapi_summoner_id_updater",
            server=self.server,
            **self.container_data["riotapi_summoner_id_updater"])
        self.container['summoner_id_updater'].startup()

        self.container['league_updater'] = Container(
            self.client,
            "riotapi_league_updater",
            server=self.server,
            **self.container_data["riotapi_league_updater"])
        self.container['league_updater'].startup()

    def _shutdown(self):
        """Shut down all container running."""
        for container in self.container:
            if container == "rabbitmq":
                continue
            self.container[container].stop()
            print("Shut down", container)

    def stop(self):
        """Set the thread to stop gracefully."""
        self._is_interrupted = True

    def run(self):
        """Run method initiating tasks."""

        self._startup()

        while not self._is_interrupted:
            # get messages
            messages = self.container['rabbitmq'].cmd(
                'rabbitmqctl list_queues  --formatter json'
            )
            status = json.loads(
                messages.output.decode('utf-8').replace("\n", ""))
            for queue in status:
                if queue['name'] == "SUMMONER_ID_IN_" + self.server:
                    if queue['messages'] < self.cutoffs['league_updater'][0]:
                        self.container['league_updater'].unpause()
                    elif queue['messages'] > self.cutoffs['league_updater'][1]:
                        self.container['league_updater'].pause()

                elif queue['name'] == "DB_MATCH_HISTORY_IN_" + self.server:
                    if queue['messages'] < \
                            self.cutoffs['summoner_id_updater'][0]:
                        self.container['summoner_id_updater'].unpause()
                    elif queue['messages'] > \
                            self.cutoffs['summoner_id_updater'][1]:
                        self.container['summoner_id_updater'].pause()

                elif queue['name'] == "MATCH_IN_" + self.server:
                    if queue['messages'] < \
                            self.cutoffs['match_history_updater'][0]:
                        self.container['match_history_updater'].unpause()
                    elif queue['messages'] > \
                            self.cutoffs['match_history_updater'][1]:
                        self.container['match_history_updater'].pause()

            time.sleep(5)

        self._shutdown()
        print("Exiting", self.server)

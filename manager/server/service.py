"""Worker services of the api.

All microservices that do not have their own data storage,
but dependencies instead.
"""
import os

from container import Container
import json

class Services:

    def __init__(self,
                 server,
                 dockerclient,
                 rabbitmq,
                 config):
        self.server = server
        self.client = dockerclient
        self.container_data = config['container']
        self.cutoffs = config['cutoffs']
        self.rabbitmq = rabbitmq
        self.container = {}

        self.stats = {}

    def startup(self):
        """Run all service container."""
        self.container['db_worker'] = Container(
            self.client,
            "riotapi_db_worker",
            server=self.server,
            **self.container_data['riotapi_db_worker'])
        self.container['db_worker'].startup()

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

    def shutdown(self):
        """Stop all service container."""
        for container in self.container:
            self.container[container].stop()
            print("Shut down", container)

    def get_state(self):
        """Return the state of all service container."""
        pass

    def watchdog(self):
        """Check all container to assure they are running."""
        pass
    def validate_existence(self):
        """Check if a container is needed or can be paused."""
        messages = self.rabbitmq.cmd(
            'rabbitmqctl list_queues --formatter json')
        status = json.loads(
            messages.output.decode('utf-8').replace('\n', ''))

        for queue in status:
            name = queue['name']
            messages = queue['messages']
            if name == "SUMMONER_ID_IN_" + self.server:
                if messages < self.cutoffs['league_updater'][0]:
                    self.container['league_updater'].unpause()
                elif messages > self.cutoffs['league_updater'][1]:
                    self.container['league_updater'].pause()

            elif name == "SUMMONER_ID_IN_" + self.server:
                if messages < self.cutoffs['league_updater'][0]:
                    self.container['league_updater'].unpause()
                elif messages > self.cutoffs['league_updater'][1]:
                    self.container['league_updater'].pause()

            elif name == "MATCH_HISTORY_IN_" + self.server:
                if messages < self.cutoffs['summoner_id_updater'][0]:
                    self.container['summoner_id_updater'].unpause()
                elif messages > self.cutoffs['summoner_id_updater'][1]:
                    self.container['summoner_id_updater'].pause()

            elif name == "MATCH_IN_" + self.server:
                if messages < self.cutoffs['match_history_updater'][0]:
                    self.container['match_history_updater'].unpause()
                elif messages > self.cutoffs['match_history_updater'][1]:
                    self.container['match_history_updater'].pause()


class Proxy:
    def __init__(self,
                 server,
                 dockerclient,
                 config):
        self.server = server
        self.client = dockerclient
        self.container_data = config['container']
        self.cutoffs = config['cutoffs']
        self.container = None
        self.api_key = os.environ['API_KEY']
        self.stats = {}


    def startup(self):
        """Run all service container."""
        self.container = Container(
            self.client,
            "riotapi_proxy",
            server=self.server,
            api_key=self.api_key,
            **self.container_data['riotapi_proxy'])
        self.container.startup(wait_for="0.0.0.0:8000")

    def shutdown(self):
        """Stop all service container."""
        self.container.stop()
        print("Shut down proxy.")

    def get_state(self):
        """Return the state of all service container."""
        pass

    def watchdog(self):
        """Check all container to assure they are running."""
        pass
    def get_request_count(self):
        """Get the most recent stats on the requests the proxy has made."""
        pass

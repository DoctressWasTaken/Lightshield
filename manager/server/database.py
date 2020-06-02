from container import Container


class Databases:

    def __init__(self,
                 server,
                 dockerclient,
                 postgres,
                 config):
        self.server = server
        self.client = dockerclient
        self.container_data = config['container']
        self.container = {}
        self.postgres = postgres
        self.stats = {}

    def startup(self):
        """Run all database container."""
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

    def shutdown(self):
        """Stop all database container."""
        for container in self.container:
            self.container[container].stop()
            print("Shut down", container)

    def get_state(self):
        """Return the state of all database container."""
        pass

    def watchdog(self):
        """Check all container to assure they are running."""
        pass

    def get_data_size(self):
        """Check the database current data size."""
        data = {}
        try:
            res = self.postgres.cmd(
                'psql -U postgres -d data_euw1 -c "SELECT COUNT(*) FROM PLAYER;"')
            player = int(res.output.decode('utf-8').split('\n')[2])
            res = self.postgres.cmd(
                'psql -U postgres -d data_euw1 -c'
                ' "SELECT COUNT(*) FROM MATCHDTO;"')
            matches = int(res.output.decode('utf-8').split('\n')[2])
            data['postgres'] = {'player': player, 'matches': matches}
            res = self.container['redis_match'].cmd(
                'redis-cli --raw scard matches')
            matches = int(res.output.decode('utf-8').strip())
            data['match'] = matches

            res = self.container['redis_summoner_id'].cmd(
                'redis-cli INFO'
            )
            summoners = int(
                res.output.decode('utf-8').strip().split('\n')[-1].split("=")[
                    1].split(',')[0])
            data['summoner_id'] = summoners

            res = self.container['redis_match_history'].cmd(
                'redis-cli INFO'
            )
            summoner = int(
                res.output.decode('utf-8').strip().split('\n')[-1].split("=")[
                    1].split(',')[0])
            data['match_history'] = summoners
        except Exception as err:
            print(err)
        return data

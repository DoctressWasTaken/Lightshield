import os
import json
import pika
import threading
import time
import psycopg2

if 'SERVER' not in os.environ:
    print("No server provided, exiting.")
    exit()
server = os.environ['SERVER']

map_tiers = {
    'IRON': 0,
    'BRONZE': 4,
    'SILVER': 8,
    'GOLD': 12,
    'PLATINUM': 16,
    'DIAMOND': 20,
    'MASTER': 24,
    'GRANDMASTER': 24,
    'CHALLENGER': 24}
map_tiers_numeric = {
    'IRON': 0,
    'BRONZE': 1,
    'SILVER': 2,
    'GOLD': 3,
    'PLATINUM': 4,
    'DIAMOND': 5,
    'MASTER': 6,
    'GRANDMASTER': 7,
    'CHALLENGER': 8}
map_rank = {
    'IV': 0,
    'III': 1,
    'II': 2,
    'I': 3}


class WorkerClass(threading.Thread):

    def __init__(self, **kwargs):
        self.thread = threading.Thread.__init__(self)
        self._is_interrupted = False
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

    def run(self):
        while not self._is_interrupted:
            pass

    def stop(self):
        self._is_interrupted = True


class UpdateSummoner(WorkerClass):

    def get_tasks(self, channel):
        """Get tasks from rabbitmq."""
        tasks = []
        while len(tasks) < 250:
            message = channel.basic_get(
                queue=f'DB_SUMMONER_IN_{server}'
            )
            if all(x is None for x in message):
                break
            tasks.append(message)
        return tasks

    def insert(self, tasks):
        """Insert the pulled tasks into the db."""
        lines = []
        for task in tasks:
            data = json.loads(task[2])
            ranking = map_tiers[data['tier']]
            ranking += map_rank[data['rank']]
            ranking += data['leaguePoints']
            tier = map_tiers_numeric[data['tier']]
            series = None
            if 'miniSeries' in data:
                series = data['miniSeries']['progress'][:-1]
            line = "('%s', '%s', '%s', '%s', %s, %s, '%s', %s, %s)"
            line = line % (
                data['summonerName'],
                data['summonerId'],
                data['accountId'],
                data['puuid'],
                ranking,
                tier,
                series,
                data['wins'],
                data['losses']
            )
            lines.append(line)
            print(f"Inserting {len(line)} lines.")
        with psycopg2.connect(
                host='postgres',
                user='db_worker',
                dbname=f'data_{server.lower()}') as connection:
            cur = connection.cursor()
            cur.execute(f"""
                               INSERT INTO player
                                   (summonerName, summonerId, accountId, 
                                   puuid, ranking, tier, series, wins, losses)
                               VALUES {",".join(lines)}
                               ON CONFLICT (summonerId) DO UPDATE SET
                                   summonerName = EXCLUDED.summonerName,
                                   ranking = EXCLUDED.ranking,
                                   tier = EXCLUDED.tier,
                                   series = EXCLUDED.series,
                                   wins = EXCLUDED.wins,
                                   losses = EXCLUDED.losses;
                           """
                        )
            print("Insert")
            connection.commit()

    def ack_tasks(self, tasks, channel):
        """Acknowledge the tasks being done towards rabbitmq."""
        for task in tasks:
            channel.basic_ack(
                delivery_tag=task[0].delivery_tag)

    def run(self):
        print("Initiated Summoner Updater.")
        while not self._is_interrupted:  # Try loop
            try:
                with pika.BlockingConnection(
                        pika.ConnectionParameters(
                            'rabbitmq',
                            connection_attempts=2,
                            socket_timeout=30)) as rabbit_connection:
                    channel = rabbit_connection.channel()
                    channel.basic_qos(prefetch_count=1)
                    # Incoming
                    queue = channel.queue_declare(
                        queue=f'DB_SUMMONER_IN_{server}',
                        durable=True)

                    while not self._is_interrupted:  # Work loop
                        tasks = self.get_tasks(channel)
                        if len(tasks) == 0:
                            print("[Summoner] No tasks, sleeping")
                            time.sleep(5)
                            continue
                        self.insert(tasks)
                        self.ack_tasks(tasks, channel)
                        time.sleep(1)

            except RuntimeError:
                # Raised when rabbitmq cant connect
                print("Failed to reach rabbitmq")
                time.sleep(1)
            except Exception as err:
                print(f"Got {err}.")
                time.sleep(1)


class InsertMatch(WorkerClass):

    def get_tasks(self, channel):
        """Get tasks from rabbitmq."""
        tasks = []
        while len(tasks) < 250:
            message = channel.basic_get(
                queue=f'DB_MATCH_IN_{server}'
            )
            if all(x is None for x in message):
                break
            tasks.append(message)
        return tasks

    def insert(self, tasks):
        """Insert the pulled tasks into the db."""

        lines = []
        for task in tasks:
            data = json.loads(task[2])
            types = {
                'CUSTOM_GAME': 0,
                'TUTORIAL_GAME': 1,
                'MATCHED_GAME': 2
            }
            gameType = 3
            if data['gameType'] in types:
                gameType = types[data['gameType']]

            line = "(%s, '%s', %s, %s, %s, '%s', '%s', %" \
                   "s, %s, '%s', %s, '%s', '%s')"
            line = line % (
                data['gameId'],
                json.dumps(data['participantIdentities']),
                data['queueId'],
                gameType,
                data['gameDuration'],
                json.dumps(data['teams']),
                data['platformId'],
                data['gameCreation'],
                data['seasonId'],
                data['gameVersion'],
                data['mapId'],
                data['gameMode'],
                json.dumps(data['participants'])
            )
            lines.append(line)
        with psycopg2.connect(
                host='postgres',
                user='db_worker',
                dbname=f'data_{server.lower()}') as connection:
            cur = connection.cursor()
            cur.execute(f"""
                                INSERT INTO matchdto
                                    (matchId, participantIdentities, queue,
                                        gameType, gameDuration, teams, platformId,
                                        gameCreation, seasonId, gameVersion, mapId,
                                        gameMode, participants)
                                VALUES {",".join(lines)}
                                ON CONFLICT (matchId) DO NOTHING;
                            """
                        )
            connection.commit()

    def ack_tasks(self, tasks, channel):
        """Acknowledge the tasks being done towards rabbitmq."""
        for task in tasks:
            channel.basic_ack(
                delivery_tag=task[0].delivery_tag)

    def run(self):
        print("Initiated Match inserter.")
        while not self._is_interrupted:  # Try loop
            try:
                with pika.BlockingConnection(
                        pika.ConnectionParameters(
                            'rabbitmq')) as rabbit_connection:
                    channel = rabbit_connection.channel()
                    channel.basic_qos(prefetch_count=1)
                    # Incoming
                    queue = channel.queue_declare(
                        queue=f'DB_MATCH_IN_{server}',
                        durable=True)
                    while not self._is_interrupted:  # Work loop
                        tasks = self.get_tasks(channel)
                        if len(tasks) == 0:
                            print("[Matches] No tasks, sleeping")
                            time.sleep(5)
                            continue
                        self.insert(tasks)
                        self.ack_tasks(tasks, channel)

            except RuntimeError:
                # Raised when rabbitmq cant connect
                print("Failed to reach rabbitmq")
                time.sleep(1)
            except Exception as err:
                print(f"Got {err}.")
                time.sleep(1)


def main():
    """Update user match lists.

    Wrapper function that starts the cycle.
    Pulls data from the DB in syncronous setup,
    calls requests in async method and uses the returned values to update.
    """
    # Pull data package
    summoner_updater = UpdateSummoner()
    summoner_updater.start()

    match_inserter = InsertMatch()
    match_inserter.start()

    try:
        while True:
            time.sleep(5)
            if not summoner_updater.is_alive():
                print("Summoner Updater Thread died. Restarting.")
                summoner_updater.start()
            if not match_inserter.is_alive():
                print("Match Inserter Thread dead. Restarting.")
                match_inserter.start()

    except KeyboardInterrupt:
        print("Gracefully shutting down.")
        summoner_updater.stop()
        match_inserter.stop()
    summoner_updater.join()
    match_inserter.join()


if __name__ == "__main__":
    main()

import json
import time
import logging

import pika

from .templates import WorkerClass

logging.basicConfig(format='%(asctime)s [SUMMONER] %(message)s',
                    level=logging.INFO)

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


class UpdateSummoner(WorkerClass):

    def get_tasks(self, channel):
        """Get tasks from rabbitmq."""
        tasks = []
        while len(tasks) < 250:
            message = channel.basic_get(
                queue=f'DB_SUMMONER_IN_{self.server}'
            )
            if all(x is None for x in message):
                break
            tasks.append(message)
        return tasks

    def insert(self, tasks):
        """Insert the pulled tasks into the db."""
        lines = []
        currentIds = []
        for task in tasks:
            data = json.loads(task[2])
            if data['summonerId'] in currentIds:
                continue
            currentIds.append(data['summonerId'])
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
            logging.info(f"Inserting {len(line)} lines.")
        with psycopg2.connect(
                host='postgres',
                user='db_worker',
                dbname=f'data_{self.server.lower()}') as connection:
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
            connection.commit()

    def ack_tasks(self, tasks, channel):
        """Acknowledge the tasks being done towards rabbitmq."""
        for task in tasks:
            channel.basic_ack(
                delivery_tag=task[0].delivery_tag)

    def run(self):
        logging.info("Initiated.")
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
                        queue=f'DB_SUMMONER_IN_{self.server}',
                        durable=True)

                    while not self._is_interrupted:  # Work loop
                        tasks = self.get_tasks(channel)
                        if len(tasks) == 0:
                            logging.info("No tasks, sleeping")
                            time.sleep(5)
                            continue
                        self.insert(tasks)
                        self.ack_tasks(tasks, channel)
                        time.sleep(1)

            except RuntimeError:
                # Raised when rabbitmq cant connect
                logging.info("Failed to reach rabbitmq")
                time.sleep(1)
            except Exception as err:
                logging.info(f"Got {err}.")
                time.sleep(1)

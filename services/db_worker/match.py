import json
import logging
import time

import pika
import psycopg2

from templates import WorkerClass

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s [MATCH] %(message)s'))
log.addHandler(ch)

class InsertMatch(WorkerClass):


    def get_tasks(self, channel):
        """Get tasks from rabbitmq."""
        tasks = []
        while len(tasks) < 750:
            message = channel.basic_get(
                queue=f'DB_MATCH_IN_{self.server}'
            )
            if all(x is None for x in message):
                if len(tasks) > 100:
                    break
                log.info("Waiting for tasks.")
                time.sleep(1)
                continue
            tasks.append(message)
        return tasks

    def insert(self, tasks):
        """Insert the pulled tasks into the db."""
        log.info(f"Inserting {len(tasks)} tasks.")
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
                host=self.postgres_host,
                user=self.postgres_user,
                port=self.postgres_port,
                dbname=f'data_{self.server.lower()}') as connection:
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
        log.info("Initiated.")
        while not self._is_interrupted:  # Try loop
            try:
                with pika.BlockingConnection(
                        pika.ConnectionParameters(
                            'rabbitmq')) as rabbit_connection:
                    channel = rabbit_connection.channel()
                    channel.basic_qos(prefetch_count=1)
                    # Incoming
                    queue = channel.queue_declare(
                        queue=f'DB_MATCH_IN_{self.server}',
                        durable=True)
                    while not self._is_interrupted:  # Work loop
                        tasks = self.get_tasks(channel)
                        if len(tasks) == 0:
                            log.info("No tasks, sleeping")
                            time.sleep(5)
                            continue
                        self.insert(tasks)
                        self.ack_tasks(tasks, channel)

            except RuntimeError:
                # Raised when rabbitmq cant connect
                log.info("Failed to reach rabbitmq")
                time.sleep(1)
            except Exception as err:
                log.info(f"Got {err}.")
                time.sleep(1)

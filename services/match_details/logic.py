"""Match History updater. Pulls matchlists for all player."""
import asyncio
import logging
import os
import traceback
from datetime import datetime, timedelta

import aiohttp
import aioredis
import asyncpg
from exceptions import RatelimitException, NotFoundException, Non200Exception
from helper import format_queue
from runes import get_ids, get_trees

shard_id = {5001: 1, 5002: 2, 5003: 3, 5005: 2, 5007: 3, 5008: 1}


class Service:
    """Core service worker object."""

    queues = None

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("MatchDetails")
        level = logging.INFO
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [MatchDetails] %(message)s")
        )
        self.logging.addHandler(handler)

        self.rune_ids = get_ids()
        self.rune_tree = get_trees()
        self.db_host = os.environ["DB_HOST"]

        self.server = os.environ["SERVER"]
        self.batch_size = int(os.environ["BATCH_SIZE"])
        self.stopped = False
        self.retry_after = datetime.now()
        self.url = (
                f"http://{self.server.lower()}.api.riotgames.com/lol/"
                + "match/v4/matches/%s"
        )

        self.buffered_elements = (
            {}
        )  # Short term buffer to keep track of currently ongoing requests

        self.active_tasks = []

    async def init(self):
        """Initiate timelimit for pulled matches."""
        self.redis = await aioredis.create_redis_pool(("redis", 6379), encoding="utf-8")
        self.logging.info("Initialized.")

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def prepare_calls(self, conn):
        template = """
        INSERT INTO team
            (match_id, side, bans, tower_kills, inhibitor_kills,
             first_tower, first_rift_herald, first_dragon, first_baron, 
             rift_herald_kills, dragon_kills, baron_kills)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT DO NOTHING;
        """
        self.team_insert = await conn.prepare(template)

        template = """
                        INSERT INTO participant
        (match_id, participant_id, summoner_id, summoner_spell,
         rune_main_tree, rune_sec_tree, rune_main_select,
         rune_sec_select, rune_shards, item, -- 10 
         trinket, champ_level, champ_id, kills, deaths, assists, gold_earned,
         neutral_minions_killed, neutral_minions_killed_enemy, 
         neutral_minions_killed_team, total_minions_killed, 
         vision_score, vision_wards_bought, wards_placed,
         wards_killed, physical_taken, magical_taken, true_taken, 
         damage_mitigated, physical_dealt, magical_dealt, 
         true_dealt, turret_dealt, objective_dealt, total_heal,
         total_units_healed, time_cc_others, total_cc_dealt)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
                $31,$32,$33,$34,$35,$36,$37,$38)
        ON CONFLICT DO NOTHING
        """
        self.participant_insert = await conn.prepare(template)

        self.match_update = await conn.prepare(
            """
        UPDATE match
            SET duration = $1,
                win = $2,
                details_pulled = TRUE
            WHERE match_id = $3
        """
        )

    async def flush_manager(self, match_details, conn):
        """Update entries in postgres once enough tasks are done."""
        try:
            match_ids = []
            for match in match_details:
                match_ids.append(match[0])

            existing_ids = [
                match["match_id"]
                for match in await conn.fetch(
                    """
                SELECT DISTINCT match_id
                FROM team
                WHERE match_id IN (%s);
                """
                    % ",".join(match_ids)
                )
            ]
            team_sets = []
            participant_sets = []
            update_sets = []
            for match in match_details:
                if not match[1]:
                    continue
                if match[0] in existing_ids:
                    continue
                details = match[1]
                # Team Details
                update_sets.append(
                    (
                        details["gameDuration"],
                        details["teams"][0]["win"] == "Win",
                        int(match[0]),
                    )
                )
                for team in details["teams"]:
                    bans = [ban["championId"] for ban in team["bans"]]
                    team_sets.append(
                        (
                            int(match[0]),
                            team["teamId"] == 200,
                            bans,
                            team["towerKills"],
                            team["inhibitorKills"],
                            team["firstTower"],
                            team["firstRiftHerald"],
                            team["firstDragon"],
                            team["firstBaron"],
                            team["riftHeraldKills"],
                            team["dragonKills"],
                            team["baronKills"],
                        )
                    )
                participants = {}
                for entry in details["participants"]:
                    participants[entry["participantId"]] = entry
                for entry in details["participantIdentities"]:
                    participants[entry["participantId"]].update(entry)

                for participant in participants.values():
                    try:
                        participant_sets.append(
                            (
                                int(match[0]),
                                participant["participantId"],
                                participant["player"]["summonerId"],
                                [participant["spell1Id"], participant["spell2Id"]],
                                self.rune_tree[participant["stats"]["perk0"]],
                                self.rune_tree[participant["stats"]["perk4"]],
                                self.rune_ids[participant["stats"]["perk0"]]
                                + self.rune_ids[participant["stats"]["perk1"]]
                                + self.rune_ids[participant["stats"]["perk2"]]
                                + self.rune_ids[participant["stats"]["perk3"]],
                                self.rune_ids[participant["stats"]["perk4"]]
                                + self.rune_ids[participant["stats"]["perk5"]],
                                shard_id[participant["stats"]["statPerk0"]] * 100
                                + shard_id[participant["stats"]["statPerk1"]] * 10
                                + shard_id[participant["stats"]["statPerk2"]],
                                [
                                    participant["stats"]["item0"],
                                    participant["stats"]["item1"],
                                    participant["stats"]["item2"],
                                    participant["stats"]["item3"],
                                    participant["stats"]["item4"],
                                    participant["stats"]["item5"],
                                ],
                                participant["stats"]["item6"],
                                participant["stats"]["champLevel"],
                                participant["championId"],
                                participant["stats"]["kills"],
                                participant["stats"]["deaths"],
                                participant["stats"]["assists"],
                                participant["stats"]["goldEarned"],
                                participant["stats"]["neutralMinionsKilled"],
                                participant["stats"]["neutralMinionsKilledEnemyJungle"],
                                participant["stats"]["neutralMinionsKilledTeamJungle"],
                                participant["stats"]["totalMinionsKilled"],
                                participant["stats"]["visionScore"],
                                participant["stats"]["visionWardsBoughtInGame"],
                                participant["stats"]["wardsPlaced"],
                                participant["stats"]["wardsKilled"],
                                participant["stats"]["physicalDamageTaken"],
                                participant["stats"]["magicalDamageTaken"],
                                participant["stats"]["trueDamageTaken"],
                                participant["stats"]["damageSelfMitigated"],
                                participant["stats"]["physicalDamageDealtToChampions"],
                                participant["stats"]["magicDamageDealtToChampions"],
                                participant["stats"]["trueDamageDealtToChampions"],
                                participant["stats"]["damageDealtToTurrets"],
                                participant["stats"]["damageDealtToObjectives"],
                                participant["stats"]["totalHeal"],
                                participant["stats"]["totalUnitsHealed"],
                                participant["stats"]["timeCCingOthers"],
                                participant["stats"]["totalTimeCrowdControlDealt"],
                            )
                        )
                    except Exception as err:
                        self.logging.info(int(match[0]))
                        raise err
            if team_sets:
                lines = []
                await self.team_insert.executemany(team_sets)

            if participant_sets:
                template = await format_queue(participant_sets[0])
                lines = []
                for line in participant_sets:
                    lines.append(
                        template
                        % tuple(
                            [
                                str(param) if type(param) in (list, bool) else param
                                for param in line
                            ]
                        )
                    )
                values = ",".join(lines)

                await self.participant_insert.executemany(participant_sets)

            if update_sets:
                await self.match_update.executemany(update_sets)
            self.logging.info("Inserted %s match_details.", len(update_sets))

        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def get_task(self):
        """Return tasks to the async worker."""
        if not (
                tasks := await self.redis.spop(
                    "%s_match_details_tasks" % self.server, self.batch_size
                )
        ):
            return tasks
        if self.stopped:
            return
        start = int(datetime.utcnow().timestamp())
        for entry in tasks:
            await self.redis.zadd(
                "%s_match_details_in_progress" % self.server, start, entry
            )
        return tasks

    async def worker(self, matchId, session, delay) -> list:
        """Multiple started per separate processor.
        Does calls continuously until it reaches an empty page."""
        await asyncio.sleep(0.8 / self.batch_size * delay)
        while not self.stopped:
            if datetime.now() < self.retry_after:
                delay = max(0.5, (self.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                return [
                    matchId,
                    await self.fetch(session=session, url=self.url % matchId),
                ]
            except NotFoundException:
                return [matchId, None]
            except (Non200Exception, RatelimitException):
                continue
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
        return [matchId, None]

    async def async_worker(self):
        afk_alert = False
        conn = await asyncpg.connect(
            "postgresql://%s@%s/%s"
            % (self.server.lower(), self.db_host, self.server.lower())
        )
        await self.prepare_calls(conn)
        while not self.stopped:
            if not (tasks := await self.get_task()):
                if not afk_alert:
                    self.logging.info("Found no tasks.")
                    afk_alert = True
                await asyncio.sleep(10)
                continue
            afk_alert = False
            async with aiohttp.ClientSession() as session:
                results = await asyncio.gather(
                    *[
                        asyncio.create_task(
                            self.worker(matchId=matchId, session=session, delay=index)
                        )
                        for index, matchId in enumerate(tasks)
                    ]
                )
            if conn.is_closed():
                conn = await asyncpg.connect(
                    "postgresql://%s@%s/%s"
                    % (self.server.lower(), self.db_host, self.server.lower())
                )
                await self.prepare_calls(conn)
            await self.flush_manager(results, conn)
            await asyncio.sleep(0.01)
        await conn.close()

    async def fetch(self, session, url) -> dict:
        """
        Execute call to external target using the proxy server.

        Receives aiohttp session as well as url to be called.
        Executes the request and returns either the content of the
        response as json or raises an exeption depending on response.
        :param session: The aiohttp Clientsession used to execute the call.
        :param url: String url ready to be requested.

        :returns: Request response as dict.
        :raises RatelimitException: on 429 or 430 HTTP Code.
        :raises NotFoundException: on 404 HTTP Code.
        :raises Non200Exception: on any other non 200 HTTP Code.
        """
        try:
            async with session.get(
                    url, proxy="http://lightshield_proxy_%s:8000" % self.server.lower()
            ) as response:
                await response.text()
        except aiohttp.ClientConnectionError:
            raise Non200Exception()
        if response.status in [429, 430]:
            if response.status == 429:
                self.logging.info(response.status)
            delay = 0.1
            if "Retry-After" in response.headers:
                delay = int(response.headers["Retry-After"])
            elif response.status == 429:
                delay = 1
            self.retry_after = datetime.now() + timedelta(seconds=delay)
            raise RatelimitException()
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return await response.json(content_type=None)

    async def run(self):
        """
        Runner.
        """
        await self.init()
        await self.async_worker()

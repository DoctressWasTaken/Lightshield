import logging
import os
import yaml
import asyncpg
from pprint import PrettyPrinter

pp = PrettyPrinter(indent=2, sort_dicts=True)

allowed_versions = [1.0]


class Object(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def get(self):
        filtered_dict = {key: val for key, val in self.__dict__.items() if
                         not key.startswith('_')}
        return {
            **{
                key: val.get() if type(val) == Object else val
                for key, val in filtered_dict.items()
            }
        }

    def __repr__(self):
        return pp.pformat(self.__dict__)


class Config:
    services = version = database = proxy = rabbitmq = db = None

    mapping = {
        "europe": ("EUW1", "EUN1", "TR1", "RU"),
        "americas": ("NA1", "BR1", "LA1", "LA2"),
        "asia": ("KR", "JP1"),
        "sea": ["OC1"],
    }
    ranks = (
        "IRON",
        "BRONZE",
        "SILVER",
        "GOLD",
        "PLATINUM",
        "DIAMOND",
        "MASTER",
        "GRANDMASTER",
        "CHALLENGER",
    )
    divisions = ("I", "II", "III", "IV")

    def __init__(self):
        self._logging = logging.getLogger("Config")
        try:
            with open(os.path.join(os.getcwd(), "config.yaml")) as file:
                self.file = yaml.safe_load(file) or {}
        except FileNotFoundError:
            self._logging.error("config.yaml was not found in %s", os.getcwd())
            exit()
        self._parse_version()
        self._parse_database()
        self._parse_connections()
        self.platforms = {}
        self._parse_platforms()
        self._parse_services()
        del self.file

    def print(self):
        filtered_dict = {key: val for key, val in self.__dict__.items() if not key.startswith('_') and val}
        pp.pprint(
            {
                **{
                    key: val.get() if type(val) == Object else val
                    for key, val in filtered_dict.items()
                }
            }
        )

    def _parse_var(
            self, parent, key: str, expected_type: type = None, parent_name: str = None
    ):
        """Handle the parsing."""
        try:
            if not expected_type:
                return parent[key]
            return expected_type(parent[key])
        except KeyError:
            if parent_name:
                self._logging.error(
                    "%s has no property %s which is required.", parent_name, key
                )
                exit()
            else:
                self._logging.error(
                    "The top level property %s is missing but required.", key
                )
                exit()
        except ValueError:
            self._logging.error("Expected a %s value for %s.", type, key)
            self._logging.error(parent)
            exit()

    def _parse_version(self):
        """Parse the version element."""
        self.version = self._parse_var(self.file, "version", float)
        if self.version not in allowed_versions:
            self._logging.error(
                "The provided config version %s is not supported %s",
                self.version,
                allowed_versions,
            )
            exit()

    def _parse_database(self):
        """Parse the database element."""
        self.database = self._parse_var(self.file, "database", str)
        if self.database not in ["crate", "postgres"]:
            self._logging.error(
                "The provided database selection '%s' is not allowed %s",
                self.database,
                ["crate", "postgres"],
            )
            exit()

    def _parse_connections(self):
        """Parse the connections element."""
        connections = self._parse_var(self.file, "connections", dict)
        proxy = self._parse_var(connections, "proxy", dict, "connections")
        self.proxy = Object(
            host=self._parse_var(proxy, "host", str, "connections.proxy"),
            port=self._parse_var(proxy, "port", int, "connections.proxy"),
            protocol=self._parse_var(proxy, "protocol", str, "connections.proxy"),
        )
        self.proxy.string = "%s://%s:%s" % (
            self.proxy.protocol,
            self.proxy.host,
            self.proxy.port,
        )
        rabbitmq = self._parse_var(connections, "rabbitmq", dict, "connections")
        self.rabbitmq = Object(
            host=self._parse_var(rabbitmq, "host", str, "connections.rabbitmq"),
            port=self._parse_var(rabbitmq, "port", int, "connections.rabbitmq"),
            user=self._parse_var(rabbitmq, "user", str, "connections.rabbitmq"),
            password_env=self._parse_var(
                rabbitmq, "password_env", str, "connections.rabbitmq"
            ),
            _password=os.environ.get(
                self._parse_var(rabbitmq, "password_env", str, "connections.rabbitmq"),
                None,
            ),
        )
        self.rabbitmq._string = "amqp://%s:%s@%s:%s" % (
            self.rabbitmq.user,
            self.rabbitmq._password,
            self.rabbitmq.host,
            self.rabbitmq.port,
        )

        db_connection = self._parse_var(connections, self.database, dict, "connections")
        self.db = Object(
            host=self._parse_var(
                db_connection, "host", str, "connections.%s" % self.database
            ),
            port=self._parse_var(
                db_connection, "port", int, "connections.%s" % self.database
            ),
            user=self._parse_var(
                db_connection, "user", str, "connections.%s" % self.database
            ),
            password=os.environ.get(
                self._parse_var(
                    db_connection, "password_env", str, "connections.%s" % self.database
                ),
                None,
            ),
        )
        if self.database == "crate":
            self.db.schema = self._parse_var(
                db_connection, "schema", str, "connections.%s" % self.database
            )
        else:
            self.db.database = self._parse_var(
                db_connection, "database", str, "connections.%s" % self.database
            )

    def _parse_platforms(self):
        """Parse the platforms."""
        expected = [
            "EUW1",
            "EUN1",
            "TR1",
            "RU",
            "NA1",
            "BR1",
            "LA1",
            "LA2",
            "OC1",
            "KR",
            "JP1",
        ]
        platforms = self._parse_var(self.file, "platforms")
        self.platforms = Object(
            **{
                platform: Object(
                    disabled=self._get_deep(platforms, [platform, "disabled"], False)
                )
                for platform in expected
            }
        )
        self.active_platforms = []
        for platform, conf in self.platforms.__dict__.items():
            if not conf.disabled:
                self.active_platforms.append(platform)

    def _get_deep(self, parent, path, default):
        cur = parent
        while path:
            step = path.pop(0)
            if not cur:
                return default
            if not (cur := cur.get(step)):
                return default
        return cur

    def _parse_services(self):
        """Parse the services."""
        services = self._parse_var(self.file, "services")
        self.services = Object(
            league_ranking=Object(
                ratelimit=self._get_deep(
                    services, ["league_ranking", "ratelimit"], None
                ),
                cycle_length=self._get_deep(
                    services, ["league_ranking", "cycle_length"], 6
                ),
                ranks=Object(
                    **{
                        rank: Object(
                            disabled=self._get_deep(
                                services,
                                ["league_ranking", "ranks", rank, "disabled"],
                                False,
                            )
                        )
                        for rank in self.ranks
                    }
                ),
            ),
            puuid_collector=Object(ratelimit=self._get_deep(
                services, ["puuid_collector", "ratelimit"], None
            ), ),
            match_history=Object(
                ratelimit=self._get_deep(
                    services, ["match_history", "ratelimit"], None
                ),
                queue=self._get_deep(services, ["match_history", "queue"], None),
                type=self._get_deep(services, ["match_history", "type"], "ranked"),
                min_age=Object(
                    newer_activity=self._get_deep(
                        services, ["match_history", "min_age", "newer_activity"], 3
                    ),
                    no_activity=self._get_deep(
                        services, ["match_history", "min_age", "newer_activity"], 10
                    ),
                ),
                history=Object(
                    days=self._get_deep(
                        services, ["match_history", "history", "days"], 14
                    ),
                    matches=self._get_deep(
                        services, ["match_history", "history", "matches"], 100
                    ),
                ),
            ),
            match_details=Object(
                ratelimit=self._get_deep(
                    services, ["match_details", "ratelimit"], None
                ),
                add_users=self._get_deep(
                    services, ["match_details", "add_users"], False
                ),
                output=self._get_deep(services, ["match_details", "output"], "./data"),
            ),
            match_timeline=Object(
                ratelimit=self._get_deep(
                    services, ["match_timeline", "ratelimit"], None
                ),
                add_users=self._get_deep(
                    services, ["match_timeline", "add_users"], False
                ),
                output=self._get_deep(services, ["match_timeline", "output"], "./data"),
            ),
        )
        self.services.league_ranking.active_ranks = []
        for ranking, conf in self.services.league_ranking.ranks.__dict__.items():
            if not conf.disabled:
                self.services.league_ranking.active_ranks.append(ranking)

    def get_db_connection(self):
        return Connection(self)


class Connection:
    """Connection handler for postgres/crate."""

    db = schema = database = None

    def __init__(self, config):
        self.type = config.database
        self.con = config.db
        self.logging = logging.getLogger("Connection Handler")
        self.logging.info("Setting up a connection to a %s db.", self.type)

    async def init(self):
        """Initiate the pool."""
        if self.type == "crate":
            self.schema = self.con.schema
            return await asyncpg.create_pool(
                host=self.con.host,
                port=self.con.port,
                user=self.con.user,
                password=self.con.password,
            )
        self.database = self.con.database
        return await asyncpg.create_pool(
            host=self.con.host,
            port=self.con.port,
            user=self.con.user,
            database=self.con.database,
            password=self.con.password,
        )

    async def close(self):
        """Close the pool."""
        await self.database.close()

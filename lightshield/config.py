import logging
import os

import yaml

allowed_versions = [1.0]
import asyncpg


class Object(object):
    pass


class Config:
    version = None
    database = None
    proxy = Object()
    rabbitmq = Object()
    db = Object()
    services = None

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
        self.logging = logging.getLogger("Config")
        try:
            with open(os.path.join(os.getcwd(), "config.yaml")) as file:
                self.file = yaml.safe_load(file)
        except FileNotFoundError:
            self.logging.error("config.yaml was not found in %s", os.getcwd())
            exit()
        self._parse_version()
        self._parse_database()
        self._parse_connections()
        self.platforms = {}
        self._parse_platforms()
        self._parse_services()

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
                self.logging.error(
                    "%s has no property %s which is required.", parent_name, key
                )
                exit()
            else:
                self.logging.error(
                    "The top level property %s is missing but required.", key
                )
                exit()
        except ValueError:
            self.logging.error("Expected a %s value for %s.", type, key)
            self.logging.error(parent)
            exit()

    def _parse_version(self):
        """Parse the version element."""
        self.version = self._parse_var(self.file, "version", float)
        if self.version not in allowed_versions:
            self.logging.error(
                "The provided config version %s is not supported %s",
                self.version,
                allowed_versions,
            )
            exit()

    def _parse_database(self):
        """Parse the database element."""
        self.database = self._parse_var(self.file, "database", str)
        if self.database not in ["crate", "postgres"]:
            self.logging.error(
                "The provided database selection '%s' is not allowed %s",
                self.database,
                ["crate", "postgres"],
            )
            exit()

    def _parse_connections(self):
        """Parse the connections element."""
        connections = self._parse_var(self.file, "connections", dict)
        proxy = self._parse_var(connections, "proxy", dict, "connections")
        self.proxy.host = self._parse_var(proxy, "host", str, "connections.proxy")
        self.proxy.port = self._parse_var(proxy, "port", int, "connections.proxy")
        self.proxy.protocol = self._parse_var(
            proxy, "protocol", str, "connections.proxy"
        )
        self.proxy.string = "%s://%s:%s" % (
            self.proxy.protocol,
            self.proxy.host,
            self.proxy.port
        )
        rabbitmq = self._parse_var(connections, "rabbitmq", dict, "connections")
        self.rabbitmq.host = self._parse_var(
            rabbitmq, "host", str, "connections.rabbitmq"
        )
        self.rabbitmq.port = self._parse_var(
            rabbitmq, "port", int, "connections.rabbitmq"
        )
        self.rabbitmq.user = self._parse_var(
            rabbitmq, "user", str, "connections.rabbitmq"
        )
        self.rabbitmq.password = os.environ.get(
            self._parse_var(rabbitmq, "password_env", str, "connections.rabbitmq"), None
        )
        self.rabbitmq.string = "amqp://%s:%s@%s:%s" % (
            self.rabbitmq.user,
            self.rabbitmq.password,
            self.rabbitmq.host,
            self.rabbitmq.port)

        db_connection = self._parse_var(connections, self.database, dict, "connections")
        self.db.host = self._parse_var(
            db_connection, "host", str, "connections.%s" % self.database
        )
        self.db.port = self._parse_var(
            db_connection, "port", int, "connections.%s" % self.database
        )
        self.db.user = self._parse_var(
            db_connection, "user", str, "connections.%s" % self.database
        )
        self.db.password = os.environ.get(
            self._parse_var(
                db_connection, "password_env", str, "connections.%s" % self.database
            ),
            None,
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
        for platform in expected:
            self.platforms[platform] = {"disabled": False}
        platforms = self._parse_var(self.file, "platforms")
        if platforms:
            for key, val in platforms.items():
                if val:
                    self.platforms[key]["disabled"] = val.get("disabled", False)

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
        self.services = {
            "league_ranking": {
                "cycle_length": self._get_deep(
                    services, ["league_ranking", "cycle_length"], 6
                ),
                "ranks": {
                    rank: {
                        "disabled": self._get_deep(
                            services, ["league_ranking", "ranks", "disabled"], False
                        )
                    }
                    for rank in self.ranks
                },
            },
            "puuid_collector": {},
            "match_history": {
                "queue": self._get_deep(services, ['match_history', 'queue'], None),
                "type": self._get_deep(services, ['match_history', 'type'], 'ranked'),
                "min_age": {
                    "newer_activity": self._get_deep(
                        services, ["match_history", "min_age", "newer_activity"], 3
                    ),
                    "no_activity": self._get_deep(
                        services, ["match_history", "min_age", "newer_activity"], 10
                    ),
                },
                "history": {
                    "days": self._get_deep(
                        services, ["match_history", "history", "days"], 14
                    ),
                    "matches": self._get_deep(
                        services, ["match_history", "history", "matches"], 100
                    ),
                },
            },
            "match_details": {
                "add_users": self._get_deep(
                    services, ["match_details", "add_users"], False
                ),
                "output": self._get_deep(
                    services, ["match_details", "output"], './data'
                )
            },
            "match_timeline": {
                "add_users": self._get_deep(
                    services, ["match_timeline", "add_users"], False
                )
            },
        }

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

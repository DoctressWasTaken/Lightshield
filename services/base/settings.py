"""Config managing file.

This file contains all config variables that may be provided to services through
env variables or an optional local config.json file.
The variables have a default value and can be reached through settings.[Variable name].
Variables are overwritten as follows:
[ENV Variable] > [config file] > [Default].
"""
import json
import os


class JsonConfig:  # pragma: no cover
    """Allow to override settings by external configuration."""

    def __init__(self, config):
        """Initialize config with dictionary."""
        self._config = config

    @classmethod
    def read(cls, envvar="CONFIG_FILE", filename="config.json"):
        """Read a JSON configuration file and create a new configuration."""
        filename = os.environ.get(envvar, filename)
        directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        filename = directory + "/" + filename
        try:
            with open(filename, "r") as config_file:
                config = json.loads(config_file.read())
        except FileNotFoundError:
            config = {}

        return cls(config)

    def get(self, key, default=None):
        """Retrieve settings value for a given key."""
        value = os.environ.get(key)

        if value:
            print("Got %s from environment." % key)
            return_val = value
        elif key in self._config.keys():
            print("Got %s from config file." % key)
            return_val = self._config[key]
        else:
            return_val = default
        return return_val

    def get_bool(self, key, default):
        """Retrieve boolean settings value."""
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        return value.lower() in ("true", "t", "yes", "y")


CONFIG = JsonConfig.read()

DEBUG = CONFIG.get("DEBUG", False)

#  ##### Default
SERVER = CONFIG.get(
    "SERVER"
).lower()  # Will fail if no variable is provided (as it should)
# PROXY SERVICE
# Url for the services to reach the proxy
# If the proxy is run externally update accordingly, else can be reached through
# docker network alias (see compose-global.yaml)
PROXY_URL = CONFIG.get("PROXY_URL", "http://proxy:8080")
# Buffer DB
# Details to your redis buffer db. Connect either via IP or docker alias
# set in the compose-global.yaml
BUFFER_HOST = CONFIG.get("BUFFER_HOST", "localhost")
BUFFER_PORT = int(CONFIG.get("BUFFER_PORT", 6379))
# Persistent DB
# Details to your postgres DB for longterm data holding
PERSISTENT_HOST = CONFIG.get("PERSISTENT_HOST", "localhost")
PERSISTENT_DATABASE = CONFIG.get("PERSISTENT_DATABASE", "postgres")
PERSISTENT_PORT = int(CONFIG.get("PERSISTENT_PORT", 5432))

#  ##### Manager services
QUEUE_LIMIT = int(CONFIG.get("LIMIT", 5000))
# Task blocking duration in minutes
# Time for which services are reserved in the buffer before being released
# If this is set too high the buffer might not clear quick enough an no new tasks
# will be added
RESERVE_MINUTES = int(
    CONFIG.get("RESERVE_MINUTES", 2)
)  # Task blocking duration in minutes

#  ##### League Ranking Scraper
# Minimum duration before the next cycle starts in hours
LEAGUE_UPDATE = int(CONFIG.get("LEAGUE_UPDATE", 1))

#  ##### Match History
# Minimum time in hours after which a user will be queued to be updated again.
AGE_THRESH = int(CONFIG.get("UPDATE_AGE", 48))  # TODO: Add support (See #22)
# Minimum new matches (ranked only) after which a user will be queued to be updated again.
MATCHES_THRESH = int(CONFIG.get("MATCHES_THRESH", 10))
# Queues to be filtered for in query and response
QUEUES = CONFIG.get("QUEUES", "")

#  ##### Match Timeline/Details
BATCH_SIZE = int(CONFIG.get("BATCH_SIZE", 30))
# Details service settings
# Backlog of details pulled
# This should either be a timestamp/date or a relative delay value appropriated for postgres
MAX_AGE = CONFIG.get("MAX_AGE", "CURRENT_DATE - 45")

#  ##### Worker
WORKER = int(CONFIG.get("WORKER", 5))

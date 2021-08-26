"""Config managing file.

This file contains all config variables that may be provided to services through
env variables or an optional local config.json file.
The variables have a default value and can be reached through settings.[Variable name].
Variables are overwritten as follows:
[ENV Variable] > [config file] > [Default].
"""
import json
import logging
import os

logger = logging.getLogger("Settings")


class JsonConfig:  # pragma: no cover
    """Allow to override settings by external configuration."""

    def __init__(self, config):
        """Initialize config with dictionary."""
        self.logging = logging.getLogger("Settings")
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
            self.logging.debug("Got %s from environment: %s.", key, value)
            return_val = value
        elif key in self._config.keys():
            self.logging.debug("Got %s from config file: %s.", key, value)
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

# Universal keys
SERVER = CONFIG.get("SERVER", "").lower()
if SERVER == "":
    logger.warning("Running without a provided server value.")
# Parallel worker processes
WORKER = int(CONFIG.get("WORKER", 5))
API_KEY = CONFIG.get("API_KEY", None)
if not API_KEY:
    logger.warning("Running without a provided API key.")
# Adds prefix to redis keys in case of multiple applications
NAMESPACE = CONFIG.get("PROXY_NAMESPACE", "")

# Dependent services
# Redis Kubernetes Service
REDIS_K8_HOST = CONFIG.get("REDIS_SERVICE_HOST", "localhost")
REDIS_K8_PORT = CONFIG.get("REDIS_SERVICE_PORT", 6379)
# Redis
REDIS_HOST = CONFIG.get("REDIS_HOST", REDIS_K8_HOST)
REDIS_PORT = CONFIG.get("REDIS_PORT", REDIS_K8_PORT)
# Proxy Sync Redis (Defaults to the same as the standard redis)
PROXY_SYNC_HOST = CONFIG.get("PROXY_SYNC_HOST", REDIS_HOST)
PROXY_SYNC_PORT = CONFIG.get("PROXY_SYNC_PORT", REDIS_PORT)
## Redis
# Redis Kubernetes Service
POSTGRES_K8_HOST = CONFIG.get("POSTGRES_SERVICE_HOST", "localhost")
POSTGRES_K8_PORT = CONFIG.get("POSTGRES_SERVICE_PORT", 5432)
# Persistent Postgres DB
PERSISTENT_HOST = CONFIG.get("PERSISTENT_HOST", POSTGRES_K8_HOST)
PERSISTENT_PORT = int(CONFIG.get("PERSISTENT_PORT", POSTGRES_K8_PORT))
PERSISTENT_DATABASE = CONFIG.get("PERSISTENT_DATABASE", "postgres")
PERSISTENT_USER = CONFIG.get("PERSISTENT_USER", "postgres")
PERSISTENT_PASSWORD = CONFIG.get("PERSISTENT_PASSWORD", None)
#  Manager max queue sizes
QUEUE_LIMIT = int(CONFIG.get("LIMIT", 5000))
# Worker Task reservation duration in minutes
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

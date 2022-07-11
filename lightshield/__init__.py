import argparse
import asyncio
import importlib
import logging
import os
import shutil
import signal

import uvloop
import yaml
from dotenv import load_dotenv

from lightshield.cli_commands import crate, postgres
from pprint import PrettyPrinter

pp = PrettyPrinter(indent=4, width=160)
load_dotenv()
uvloop.install()

default_services = (
    "league_ranking",
    "puuid_collector",
    "summoner_tracker",
    "match_history",
    "match_details",
    "match_timeline",
)


class Config:
    """Config containing class."""

    def __init__(self, data):
        self.__dict__ = data
        for key in data:
            if isinstance(getattr(self, key), dict):
                setattr(self, key, Config(getattr(self, key)))

    def __repr__(self):
        return str(self.__dict__)


def parse_config():
    """Load files and init parse."""
    with open("config.yaml") as configs_file:
        configs = yaml.safe_load(configs_file)
    with open(
        os.path.join(os.path.dirname(__file__), "templates", "config.yaml")
    ) as configs_defaults:
        default = yaml.safe_load(configs_defaults)
    data = merge(configs, default)
    return Config(data)


def merge(source, destination):
    """
    >>> a = { 'first' : { 'all_rows' : { 'pass' : 'dog', 'number' : '1' } } }
    >>> b = { 'first' : { 'all_rows' : { 'fail' : 'cat', 'number' : '5' } } }
    >>> merge(b, a) == { 'first' : { 'all_rows' : { 'pass' : 'dog', 'fail' : 'cat', 'number' : '5' } } }
    True
    """
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            merge(value, node)
        elif value:
            destination[key] = value
    return destination


async def shutdown(services):
    """Init shutdown in all active services."""
    for service in services.values():
        await service.init_shutdown()


async def run(*args, config, services=None, **kwargs):
    """Import and start the select services."""
    active_services = {}
    for service in services:
        active_services[service] = importlib.import_module(
            "lightshield.services.%s" % service
        ).Handler(config)
    tasks = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(
            sig, lambda signame=sig: asyncio.create_task(shutdown(active_services))
        )
    for service in active_services.values():
        tasks.append(asyncio.create_task(service.run()))

    await asyncio.gather(*tasks)


async def init_config(**kwargs):
    src = os.path.join(os.path.dirname(__file__), "templates", "config.yaml")
    target = os.path.join(os.getcwd(), "config.yaml")
    print(src)
    shutil.copyfile(src, target)


def main():
    parser = argparse.ArgumentParser(description="Lightshield library CLI.")
    parser.add_argument(
        "-d",
        action="store_true",
        help="Set debug output. Also triggered by DEBUG=True env flag.",
    )
    subparsers = parser.add_subparsers(help="List of commands runnable")
    subparsers.required = True
    _run = subparsers.add_parser("run", help="Run one or multiple services")
    _run.set_defaults(func=run)
    _run.add_argument("services", nargs="+", choices=default_services)

    _init_config = subparsers.add_parser(
        "init-config", help="Generate a config file if none exists."
    )
    _init_config.set_defaults(func=init_config, init_config=True)

    _init_postgres = subparsers.add_parser("init-postgres", help="Initialize the Postgres DB")
    _init_postgres.set_defaults(func=postgres.init_db)

    _init_crate = subparsers.add_parser("init-crate", help="Initialize the Crate DB")
    _init_crate.set_defaults(func=crate.init_db)

    args = vars(parser.parse_args())
    if "DEBUG" in os.environ or args.get("d"):
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(levelname)8s %(asctime)s %(name)15s| %(message)s",
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)8s %(asctime)s %(name)15s| %(message)s",
        )
    # TODO: Add an option for a changed config name.
    config = None
    if not args.get("init_config"):
        try:
            with open("config.yaml"):
                pass
        except FileNotFoundError:
            logging.error(
                "The config file was not found, please run `lightshield generate-config` first to generate it."
            )
            exit()
        config = parse_config()
    asyncio.run(args.get("func")(config=config, **args))

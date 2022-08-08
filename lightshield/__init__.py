import argparse
import asyncio
import importlib
import logging
import os
import shutil
import signal

from dotenv import load_dotenv

from lightshield.cli_commands import postgres
from pprint import PrettyPrinter
from lightshield.config import Config

pp = PrettyPrinter(indent=2, compact=True, sort_dicts=True)
load_dotenv()

default_services = (
    "tracker",

    "league_ranking",
    "puuid_collector",
    "summoner_tracker",
    "match_history",
    "match_history.rabbitmq.tasks",
    "match_history.rabbitmq.results",
    "match_details",
    "match_details.rabbitmq.tasks",
    "match_details.rabbitmq.results",
    "match_timeline",
    "puuid_collector.rabbitmq.tasks",
    "puuid_collector.rabbitmq.results",
)


async def shutdown(services):
    """Init shutdown in all active services."""
    logging.info("Shutting down.")
    for service in services.values():
        await service.init_shutdown()


async def print_config(*args, **kwargs):
    """Print out the configs."""
    conf = Config()
    conf.print()


async def run(*args, services=None, **kwargs):
    """Import and start the select services."""
    active_services = {}
    for service in services:
        active_services[service] = importlib.import_module(
            "lightshield.services.%s" % service
        ).Handler()
    tasks = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(
            sig, lambda signame=sig: asyncio.create_task(shutdown(active_services))
        )
    for service in active_services.values():
        tasks.append(asyncio.create_task(service.run()))

    await asyncio.gather(*tasks)


async def init_db(**kwargs):
    """Selector for which database to init."""
    config = Config()
    await postgres.init_db(config, **kwargs)


async def init_config(**kwargs):
    """Copy the template config file into the current directory."""
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
    _run = subparsers.add_parser(
        "run",
        help="Run one or multiple services",
        usage="Run one or multiple services using a single command. Provide multiple services separated by space \n\n\tlightshield run [-h] [service [ ...]]",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    _run.set_defaults(func=run)
    _run.add_argument("services", nargs="+", choices=default_services)

    _init = subparsers.add_parser("init", help="Generate a config file if none exists.")
    _init.set_defaults(func=init_config, init_config=True)

    _init_db = subparsers.add_parser(
        "init-database",
        help="Initialize the database type as selected in the config.yaml.",
    )
    _init_db.set_defaults(func=init_db)

    config = subparsers.add_parser("config", help="Print out the current config.")
    config.set_defaults(func=print_config, init_config=True)

    args = vars(parser.parse_args())
    if ("DEBUG" in os.environ and os.environ.get("DEBUG", None) == "True") or args.get(
        "d"
    ):
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
    asyncio.run(args.get("func")(**args))

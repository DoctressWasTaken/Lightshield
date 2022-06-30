import sys
import asyncio
import argparse
import os
import logging
import uvloop
import importlib
import shutil
import yaml
from lightshield.postgres import init_db
from dotenv import load_dotenv

load_dotenv()
uvloop.install()

default_services = (
    "league_ranking",
    "summoner_id",
    "match_history",
    "match_details",
    "match_timeline",
)


async def run(*args, configs, services=None, **kwargs):
    """Import and start the select services."""
    active_services = {}
    for service in services:
        active_services[service] = importlib.import_module(
            "lightshield.%s" % service
        ).Handler(configs)
    tasks = []
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

    _init_db = subparsers.add_parser("init-postgres", help="Initialize the Postgres DB")
    _init_db.set_defaults(func=init_db)

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
    if not args.get("init_config"):
        try:
            with open("config.yaml") as configs_file:
                configs = yaml.safe_load(configs_file)
        except FileNotFoundError:
            logging.error(
                "The config file was not found, please run `lightshield generate-config` first to generate it."
            )
            exit()
    asyncio.run(args.get("func")(configs=configs, **args))

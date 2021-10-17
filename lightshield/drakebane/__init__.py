"""CLI Utility to manage the Lightshield cluster through redis variables."""
import argparse
import logging

from lightshield.drakebane.platform import Platform


def run():
    """Script Entrypoint."""
    parser = argparse.ArgumentParser(
        description="CLI for the Lightshield cluster.", usage="%(prog)s [args]"
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Enable debug output."
    )
    subparsers = parser.add_subparsers()
    Platform().set_parser(parser, subparsers)

    try:
        args = parser.parse_args()
        if args.debug:
            logging.basicConfig(
                level=logging.DEBUG,
                format="%(levelname)8s %(asctime)s %(name)15s| %(message)s",
            )
        else:
            logging.basicConfig(
                level=logging.INFO,
                format="%(levelname)8s %(asctime)s %(name)15s| %(message)s",
            )
        logging.debug("Running with additional output.")
        args.func.run(args)
    except Exception as err:
        parser.print_help()

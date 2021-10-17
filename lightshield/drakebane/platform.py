import logging

from lightshield import settings


class Platform:
    """Manage platforms that are being processed by Lightshield."""

    def __init__(self):
        self.logging = logging.getLogger("Platform")

    def set_parser(self, parent, subparser):
        self.parser = subparser.add_parser(
            "platform", usage="%(prog)s [args]", help=self.__doc__
        )

        args = self.parser.add_mutually_exclusive_group()
        args.add_argument(
            "--add",
            nargs="+",
            choices=settings.statics.PLATFORMS,
            help=self.add.__doc__,
            metavar="PLATFORM",
        )
        args.add_argument("--list", action="store_true", help=self.list.__doc__)
        args.add_argument(
            "--remove",
            nargs="+",
            choices=settings.statics.PLATFORMS,
            help=self.remove.__doc__,
            metavar="PLATFORM",
        )
        self.parser.set_defaults(func=self)

    def run(self, args):
        if args.add:
            self.add()
        elif args.list:
            self.list()
        elif args.remove:
            self.remove()
        else:
            self.parser.print_help()

    def add(self):
        """Add a not yet tracked legal platform to the list of tracked platforms."""
        self.logging.debug("Add method")
        pass

    def list(self):
        """List the status of all platforms."""
        self.logging.debug("Add method")
        pass

    def remove(self):
        """Remove a currently tracked legal platform from tracking."""
        self.logging.debug("Add method")
        pass

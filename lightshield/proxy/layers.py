import logging

from .middleware import Middleware


class Server(Middleware):

    def __init__(self, server, namespace):
        self.server = server
        self.namespace = namespace
        self.key = "%s:%s" % (
            namespace,
            server,
        )
        self.limits = None
        self.header = "X-App"
        self.logging = logging.getLogger("Proxy")


class Zone(Middleware):

    def __init__(self, server, zone, namespace):
        self.zone = zone
        self.namespace = namespace
        self.key = "%s:%s:%s" % (
            namespace,
            server,
            zone,
        )
        self.limits = None
        self.header = "X-Method"
        self.logging = logging.getLogger("Proxy")

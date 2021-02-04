"""Custom Exceptions."""


class RatelimitException(Exception):
    """On 429 or 430 Response."""


class NotFoundException(Exception):
    """On 404-Response."""


class Non200Exception(Exception):
    """On Non-200 Response thats not 429, 430 or 404."""


class NoMessageException(Exception):
    """Timeout exception if no message is found."""

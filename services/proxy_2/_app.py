from datetime import datetime, timezone, timedelta
import pytz

class LimitBlocked(Exception):

    def __init__(self, retry_after):
        self.retry_after = retry_after


class LimitHandler:

    def __init__(self, span, max):
        self.span = int(span)  # Duration of the bucket
        self.max = max - 1  # Max Calls per bucket (Reduced by 1 for safety measures)
        self.count = 0  # Current Calls in this bucket
        self.bucket = datetime.now()  # Cutoff after which no new requests are accepted
        self.reset = self.bucket + timedelta(
            seconds=1)  # Moment of reset for the bucket
        self.last_updated = datetime.now()  # Latest received response
        self.blocked = datetime.now()
        print(f"Initiated {self.max}:{self.span}.")


    @property
    def open(self):
        if (datetime.now() < self.bucket or datetime.now() > self.reset)\
                and self.count < self.max:
            return True
        return False

    def check_reset(self):
        """Check if the bucket has timed out."""
        if datetime.now() > self.reset:
            self.count = 0
            self.bucket = datetime.now() + timedelta(
                seconds=self.span - 2)
            self.reset = datetime.now() + timedelta(seconds=self.span + 1)


    def accept_requests(self):
        """Check if the bucket is currently accepting requests."""
        if self.bucket < datetime.now() < self.reset:
            raise LimitBlocked(self.when_reset())
        return True


    def when_reset(self):
        """Return seconds until reset."""
        return (self.reset - datetime.now()).total_seconds()


    def register(self):
        """Reqister a request.

        Returns false if the limit is blocked, true if open.
        Adds count to limit when returning true.
        While iterating through applied limits,
        if a later limit blocks despite previous being open,
        the previous limit counts are NOT lowered.
        """
        self.check_reset()
        # Return if in dead-period
        self.accept_requests()

        # Attempt to add request
        if self.count >= self.max:
            raise LimitBlocked(self.when_reset())
        self.count += 1

class AppApiHandler:

    def __init__(self, limits):
        self.limits = [LimitHandler(limit, limits[limit]) for limit in limits]


    def register(self):
        """Register a request."""
        for limit in self.limits:
            limit.register()

    @property
    def open(self):
        """Return True if both """
        for limit in self.limits:
            if not limit.open:
                return  False
        return True
from datetime import datetime, timezone, timedelta
import pytz


class LimitBlocked(Exception):

    def __init__(self, retry_after):
        self.retry_after = retry_after


class LimitHandler:

    def __init__(self, limits=None, span=None, max=None):
        if limits:
            span, max = [int(i) for i in limits]
        self.span = int(span)  # Duration of the bucket
        self.max = max - 1  # Max Calls per bucket (Reduced by 1 for safety measures)
        self.count = 0  # Current Calls in this bucket
        self.bucket = None  # Cutoff after which no new requests are accepted

        print(f"Initiated {self.max}:{self.span}.")

    @property
    def add(self):
        # (Re)set bucket if applicable
        if not self.bucket or self.bucket + timedelta(seconds = self.span) < datetime.now(timezone.utc):
            self.bucket = datetime.now(timezone.utc)
            self.count = 0

        if self.count < self.max:
            self.count += 1
            return
        raise LimitBlocked(self.when_reset())

    def when_reset(self):
        """Return seconds until reset."""
        end = self.bucket + timedelta(seconds=self.span)

        return int((end - datetime.now(timezone.utc)).total_seconds()) + 1

    async def update(self, date, limits):
        count = [int(limit.split(":")[0]) for limit in limits if limit.endswith(str(self.span))][0]
        if count == 1:
            naive = datetime.strptime(
                date,
                '%a, %d %b %Y %H:%M:%S GMT')
            local = pytz.timezone('GMT')
            local_dt = local.localize(naive, is_dst=None)
            date = local_dt.astimezone(pytz.utc)
            if date > self.bucket:
                print(f"Corrected bucket by {date - self.bucket}.")
                self.bucket = date

        if count > self.count:
            self.count = count
            return
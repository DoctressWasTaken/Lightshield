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
        self.bucket_start = None  # Cutoff after which no new requests are accepted
        self.bucket_end = None
        self.bucket_reset_ready = None
        self.bucket_verifier = None

        print(f"Initiated {self.max}:{self.span}.")

    @property
    def add(self):
        # (Re)set bucket if applicable
        if not self.bucket_start or self.bucket_end < datetime.now(timezone.utc):
            self.bucket_start = datetime.now(timezone.utc)
            self.bucket_end = self.bucket_start + timedelta(seconds=self.span + 0.5)  # EXTRA 0.5 seconds when initiated
            self.bucket_reset_ready = self.bucket_start + timedelta(seconds=self.span * 0.8)

            self.count = 0

        if self.count < self.max:
            self.count += 1
            return
        raise LimitBlocked(self.when_reset())

    def when_reset(self):
        """Return seconds until reset."""
        return int((self.bucket_end - datetime.now(timezone.utc)).total_seconds())

    async def update(self, date, limits):
        count = [int(limit.split(":")[0]) for limit in limits if limit.endswith(str(self.span))][0]
        naive = datetime.strptime(
            date,
            '%a, %d %b %Y %H:%M:%S GMT')
        local = pytz.timezone('GMT')
        local_dt = local.localize(naive, is_dst=None)
        date = local_dt.astimezone(pytz.utc)

        if count <= 5 and date > self.bucket_start:

            if date < self.bucket_reset_ready:
                if self.bucket_verifier < count:
                    print(f"Corrected bucket by {(date - self.bucket_start).total_seconds()}.")
                    self.bucket_start = date
                    self.bucket_end = self.bucket_start + timedelta(seconds=self.span)  # No extra time cause verified
                    self.bucket_reset_ready = self.bucket_start + timedelta(seconds=self.span * 0.8)
                    self.bucket_verifier = count
            else:
                print(f"Initiated new bucket at {date}.")
                self.bucket_start = date
                self.bucket_end = self.bucket_start + timedelta(
                    seconds=self.span)  # No extra time cause verified
                self.bucket_reset_ready = self.bucket_start + timedelta(seconds=self.span * 0.8)
                self.bucket_verifier = count
                self.count = count

        elif count > 5 and date > self.bucket_start:
            if count > self.count:
                self.count = count

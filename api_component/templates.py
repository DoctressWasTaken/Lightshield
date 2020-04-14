import datetime

class Limit:

    def __init__(self, span: int, max: int):
        print(f"\t\tInitiated Limit {max} over {span}")
        self.span = span
        self.max = max
        self.current = 0
        self.bucket_start = None
        self.last_update = datetime.datetime.now()

    def is_blocked(self):
        """Return if the limit is reached."""
        self.update_bucket()
        if self.current < self.max:
            return False
        else:
            return True

    def add(self):
        self.update_bucket()
        self.current += 1

    def update_bucket(self):
        if not self.bucket_start:
            self.bucket_start = datetime.datetime.now()

        elif (datetime.datetime.now() - self.bucket_start).total_seconds() > self.span:
            self.bucket_start = datetime.datetime.now()
            self.current = 0


class DefaultEndpoint:

    methods = {}
    url = ""

    def __init__(self, limits, api):
        print(f"\tInitiating {self.__class__.__name__}")
        self.api = api
        self.limits = []
        for limit in limits:
            self.limits.append(
                Limit(span=int(limit), max=limits[limit])
            )

    async def request(self, limits: list, method: str, params: dict):
        for limit in self.limits:
            if limit.is_blocked():
                raise Exception("Blocked")
            for limit in self.limits:
                limit.add()
            for limit in limits:
                limit.add()

            if method in self.methods:
                method_params = self.methods[method]['params']
                method_url = self.methods[method]['url']
                method_codes = self.methods[method]['allowed_codes']
            else:
                print(f"Method not found. Available methods are {self.methods.keys()}.")
                raise("Missing method")
            params_list = []
            for entry in method_params:
                if entry not in params:
                    print(f"Parameter {entry} not provided. Required parameters are {method_params}")
                    raise("Missing params")
                else:
                    params_list.append(params[entry])
            url = self.url
            url += method_url % tuple(params_list)

            response = await self.api.send(url)
            if response.status not in method_codes:
                print(response.status)
                print("Not in allowed codes")
                raise Exception('Not an allowed response code')
            return await response.json()
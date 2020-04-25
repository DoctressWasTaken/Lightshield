import tornado.ioloop
import tornado.web
import time
from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer
from tornado.httpclient import AsyncHTTPClient
from tornado.concurrent import Future
import requests
import os, json
import asyncio
import datetime, pytz
from datetime import timezone
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

if "SERVER" not in os.environ:
    logging.info("No SERVER env variable provided. Exiting.")
    exit()

config = json.loads(open("config.json").read())

server = os.environ["SERVER"]
headers = {'X-Riot-Token': config['API_KEY']}
url = f"https://{server}.api.riotgames.com/lol"

class Limit():

    def __init__(self, span, count):
        self.span = int(span)
        self.max = int(count * 0.9)
        self.count = 0
        self.bucket = datetime.datetime.now(timezone.utc)
        self.last_updated = datetime.datetime.now(timezone.utc)
        self.blocked = datetime.datetime.now(timezone.utc)

    def is_blocked(self):
        print(self.count, self.max)
        if self.blocked > datetime.datetime.now(timezone.utc):
            return True
        
        if self.bucket < datetime.datetime.now(timezone.utc):  # Bucket timed out
            self.count = 0
            return False
        if self.count < self.max:  # Not maxed out
            return False
        return True

    def request(self):

        if self.blocked > datetime.datetime.now(timezone.utc):
            logging.info("Limit is blocked.")
            return False

        if self.bucket < datetime.datetime.now(timezone.utc):
            self.bucket = datetime.datetime.now(timezone.utc) + datetime.timedelta(seconds=self.span)
            logging.info(f"Starting bucket until {self.bucket}.")
            self.count = 1
            return True

        if self.count < self.max:
            self.count += 1
            logging.info(f"Adding request {self.count} : {self.max}.")
            return True

        logging.info("Already on limit")
        return False


    def sync(self, count, date, retry_after):
        print("Syncing", self.span)
        print("Last update:", self.last_updated)
        print("Date:", date)
        if self.last_updated > date:
            return
        print("Updating", self.span, self.count, count)
        self.count = count
        if self.count > self.max:
            self.blocked = datetime.datetime.now(timezone.utc) + datetime.timedelta(seconds=retry_after)
            print("Blocking for", retry_after)



class ApiHandler():

    def __init__(self):
        limits = json.loads(open("limits.json").read())
        # global limits
        self.globals = []
        for limit in limits['APP']:
            self.globals.append(
                    Limit(limit, limits['APP'][limit])
                        )
        # method limits
        self.methods = {}
        for method in limits['METHODS']:
            for limit in limits['METHODS'][method]:
                self.methods[method] = Limit(limit, limits['METHODS'][method][limit])
        

    def request(self, method):
        
        print("Try to register request")
        if not self.methods[method].request():
            return False
        for limit in self.globals:
            if not limit.request():
                return False
        return True

    def call(self, url, method):
        print("Called")
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            print(r.headers)
        
        app_current = r.headers['X-App-Rate-Limit-Count']
        method_current = r.headers['X-Method-Rate-Limit-Count']
        naive = datetime.datetime.strptime(
                r.headers['Date'],
                '%a, %d %b %Y %H:%M:%S GMT')
        local = pytz.timezone('GMT')
        local_dt = local.localize(naive, is_dst=None)
        date = local_dt.astimezone(pytz.utc)

        retry_after = 0
        if 'Retry-After' in r.headers:
            retry_after = int(r.headers['Retry-After'])

        for limit in self.globals:
            for current in app_current.split(','):
                if current.endswith(str(limit.span)):
                    limit.sync(
                            int(current.split(":")[0]),
                            date,
                            retry_after)
                    break
        self.methods[method].sync(
            int(method_current.split(":")[0]),
            date,
            retry_after)

        return {
            "body": r.json(),
            "code": r.status_code
            } 


class Proxy:

    def __init__(self):
        self.api = ApiHandler()

        params = {
            "api": self.api
            }

        app = tornado.web.Application([
            (r"/league/.*", self.LeagueHandler, params),
            (r"/summoner/.*", self.SummonerHandler, params),
            (r"/league-exp/.*", self.LeagueExpHandler, params),
            ])
        
        self.server = HTTPServer(app)

    def run(self, port):
        self.server.bind(port)
        self.server.start(0)
        IOLoop.current().start()

    def stop(self):
        self.server.stop()

    class DefaultHandler(tornado.web.RequestHandler):
        name = ""
        url = ""

        def initialize(self, *args, **kwargs):
            self.api = kwargs['api']

        def get(self):
            logging.info(f"Received request for {self.name}. ")
            if self.api.request(self.name):
                
                response = self.api.call(
                        url
                        + self.request.uri,
                        self.name)

                
                self.set_status(response['code'])
                self.finish("AAA")
                #self.finish(json.dumps(response['body']))
                return

            print("Denied")
            self.set_status(429)
            self.finish("error")



    class LeagueHandler(DefaultHandler):
        name = "league"
        
    class LeagueExpHandler(DefaultHandler):
        name = "league-exp"
        url = f"https://{server}.api.riotgames.com/lol/league-exp/"

    class SummonerHandler(DefaultHandler):
        name = "summoner"
        
if __name__ == '__main__':
    proxy = Proxy()
    try:
        proxy.run(port=8888)
    except:
        proxy.stop()

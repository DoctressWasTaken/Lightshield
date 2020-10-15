import patch_updater
import asyncio
import datetime
import os
from buffer_db import BufferDB
from permanent_db import PermanentDB
import time
import threading


def main():
    buffer = BufferDB()

    patches = asyncio.run(patch_updater.get())
    servers = {"KR": -39600,
               "EUW1": -10800,
               "NA1": 10800}
    print(patches)
    print(servers)

    timings = sorted(list(patches.keys())) + [int(datetime.datetime.now().timestamp())]
    cutoff = datetime.datetime.utcnow()

    server = os.environ['SERVER']
    permanent = PermanentDB()
    for patch in patches:
        permanent.create_db(patches[patch]['name'])

    print("Processing %s." % server)
    summoner = buffer.get_summoner(server)

    for i in range(len(timings) - 1):
        start = (timings[i] + servers[server]) * 1000
        end = (timings[i + 1] + servers[server]) * 1000
        patch = patches[timings[i]]
        print("Patch %s" % patch['name'])
        threads = []
        active_threads = [0]
        permanent.create_session(patch['name'])
        matches = []
        matchIds = [tupl[0] for tupl in permanent.get_matches(patch['name'])]
        processed_matches = 0
        for match in buffer.get_matches(server, cutoff, start, end).yield_per(5000):
            if (processed_matches := processed_matches + 1) % 5000 == 0:
                print("Processed %s matches." % processed_matches)
            while active_threads[0] >= 5:
                time.sleep(1)
            if match.matchId in matchIds:
                continue
            matches.append(match)
            if len(matches) < 1000:
                continue
            permanent.add_summoner(patch['name'], summoner, matches)
            thread = threading.Thread(
                target=permanent.add_matches, args=(patch['name'], matches.copy(), active_threads))
            thread.start()
            matches = []
            threads.append(thread)
            active_threads[0] += 1
        for thread in threads:
            thread.join()

        buffer.delete_matches(server, cutoff, end)
        permanent.close_session(patch['name'])


if __name__ == "__main__":
    while True:
        main()
        print("Finished cycle.")
        time.sleep(60)

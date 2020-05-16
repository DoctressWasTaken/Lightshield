import os
import time
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s [RUN] %(message)s'))
log.addHandler(ch)

from summoner import UpdateSummoner
from match import InsertMatch

if 'SERVER' not in os.environ:
    print("No server provided, exiting.")
    exit()
server = os.environ['SERVER']

def main():
    """Update user match lists.

    Wrapper function that starts the cycle.
    Pulls data from the DB in syncronous setup,
    calls requests in async method and uses the returned values to update.
    """
    # Pull data package
    summoner_updater = UpdateSummoner(server=server)
    summoner_updater.start()

    match_inserter = InsertMatch(server=server)
    match_inserter.start()
    try:
        while True:
            time.sleep(5)
            if not summoner_updater.is_alive():
                log.error("Summoner Updater Thread died. Restarting.")
                summoner_updater.start()
            if not match_inserter.is_alive():
                log.error("Match Inserter Thread dead. Restarting.")
                match_inserter.start()

    except KeyboardInterrupt:
        log.info("Gracefully shutting down.")
        summoner_updater.stop()
        match_inserter.stop()

    summoner_updater.join()
    match_inserter.join()


if __name__ == "__main__":
    main()

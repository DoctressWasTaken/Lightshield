import os
import time
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s [RUN] %(message)s'))
log.addHandler(ch)

from summoner import Worker as UpdateSummoner

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
    summoner_updater = UpdateSummoner()

    summoner_updater.start()

    try:
        while True:
            time.sleep(5)
            if not summoner_updater.is_alive():
                log.error("Summoner Updater Thread died. Restarting.")
                summoner_updater.start()

    except KeyboardInterrupt:
        log.info("Gracefully shutting down.")
        summoner_updater.stop()

    summoner_updater.join()


if __name__ == "__main__":
    main()

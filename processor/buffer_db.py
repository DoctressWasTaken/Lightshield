from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tables import Base, Match, Summoner
from tables.enums import Server
import time

class BufferDB:

    def __init__(self):
        """Create connection to the Buffer DB."""
        self.engine = create_engine(
        f'postgresql://%s@%s:%s/data' %
        ('db_worker',
         'postgres',
         5432),
        echo=False)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def get_summoner(self, server):
        """Export all summoner for a specific server and restructure them as dict."""
        summoners = self.session\
            .query(Summoner)\
            .filter_by(server=Server.get(server))\
            .all()

        summoners_dict = {}
        for summoner in summoners:
            summoners_dict[summoner.accountId] = {
                "rank": summoner.tier.value * 400 + summoner.rank.value * 100 + summoner.leaguePoints,
                "wins": summoner.wins,
                "losses": summoner.losses,
                "puuid": summoner.puuid
            }
        print("Found %s summoner." % len(summoners_dict))
        return summoners_dict

    def get_matches(self, server, cutoff, start, end, pagesize=500):
        """Return filtered matches.

        ::param::server: Server that the games happened on.
        ::param::cutoff: Creation date of the match database entry.
        ::param::start:: Earliest match start.
        ::param::end:: Latest match start.
        ::param::pagesize:: Number of matches returned at once
        """
        amount = self.session.query(Match)\
            .filter_by(server=server) \
            .filter(Match.gameCreation >= start) \
            .filter(Match.gameCreation <= end) \
            .filter(Match.matchAdded < cutoff) \
            .count()
        print("Found %s matches." % amount)
        return self.session.query(Match)\
                .filter_by(server=server) \
                .filter(Match.gameCreation >= start) \
                .filter(Match.gameCreation <= end) \
                .filter(Match.matchAdded < cutoff)

    def delete_matches(self, server, cutoff, end):
        """Delete selected matches.

        Start parameter not relevant as the patches are updated old -> new.
        ::param::server: Server that the games happened on.
        ::param::cutoff: Creation date of the match database entry.
        ::param::start:: Earliest match start.
        ::param::end:: Latest match start.
        """
        print("Deleting processed matches. [%s, %s]" % (server, end))
        self.session.query(Match) \
            .filter_by(server=server) \
            .filter(Match.gameCreation <= end) \
            .filter(Match.matchAdded < cutoff) \
            .delete()
        self.session.commit()

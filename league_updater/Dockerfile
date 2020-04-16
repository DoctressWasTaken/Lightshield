FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip
COPY league_updater/requirements.txt league_updater/requirements.txt
RUN pip install -r league_updater/requirements.txt

COPY league_updater/ ./league_updater/
COPY config.json .

WORKDIR /project/league_updater
RUN chmod 500 startup.sh

CMD ["/project/league_updater/startup.sh"]

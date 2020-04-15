FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip
COPY league_updater/requirements.txt league_updater/requirements.txt
RUN pip install -r api_component/requirements.txt
COPY config.json .

COPY league_updater/ ./league_updater/

WORKDIR /project/league_updater
RUN chmod 500 startup.sh

CMD ["/project/league_updater/startup.sh"]

FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip
# Main Application
COPY summoner_id_updater/requirements.txt summoner_id_updater/requirements.txt
RUN pip install -r summoner_id_updater/requirements.txt
COPY summoner_id_updater/ ./summoner_id_updater/
COPY config.json .

WORKDIR /project/summoner_id_updater
# Data module
RUN chmod 500 startup.sh

CMD ["/project/summoner_id_updater/startup.sh"]

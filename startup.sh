#docker-compose -f docker-compose-dev.yaml down
#docker volume rm riotapi_rabbitmq
#docker volume rm riotapi_playerdata
docker-compose -f docker-compose-dev.yaml up -d rabbitmq playerdata
docker-compose -f docker-compose-dev.yaml build api_component league_updater_euw summoner_id_updater_euw
sleep 15
docker-compose -f docker-compose-dev.yaml up api_component league_updater_euw summoner_id_updater_euw
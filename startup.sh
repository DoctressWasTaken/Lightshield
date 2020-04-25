#docker-compose -f docker-compose-dev.yaml down
docker-compose -f docker-compose-dev.yaml build
sleep 15
docker-compose -f docker-compose-dev.yaml up
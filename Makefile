

br:
	docker-compose -f docker-compose-dev.yaml build $(filter-out $@, $(MAKECMDGOALS))
	docker-compose -f docker-compose-dev.yaml up $(filter-out $@, $(MAKECMDGOALS))

brd:
	docker-compose -f docker-compose-dev.yaml build $(filter-out $@, $(MAKECMDGOALS))
	docker-compose -f docker-compose-dev.yaml up -d $(filter-out $@, $(MAKECMDGOALS))

manager-run:
	docker-compose -f docker-compose-dev.yaml -p riotapi build --parallel
	docker-compose -f docker-compose-dev.yaml -p riotapi up -t 180 manager

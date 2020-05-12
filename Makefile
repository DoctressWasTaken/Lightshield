

br:
	docker-compose -f docker-compose-dev.yaml build $(filter-out $@, $(MAKECMDGOALS))
	docker-compose -f docker-compose-dev.yaml up $(filter-out $@, $(MAKECMDGOALS))

brd:
	docker-compose -f docker-compose-dev.yaml build $(filter-out $@, $(MAKECMDGOALS))
	docker-compose -f docker-compose-dev.yaml up -d $(filter-out $@, $(MAKECMDGOALS))




brun:
	docker-compose -f docker-compose-dev.yaml build $(filter-out $@, $(MAKECMDGOALS))
	docker-compose -f docker-compose-dev.yaml up $(filter-out $@, $(MAKECMDGOALS))

# Docker


Using Lightshield through Docker allows easier communication as well as scaling of certain services.

Expected env variables:  
`API_KEY`: 
   Riot API Key(s). Multiple keys can be provided by using a `|` to separate them however they need to share the same obfuscation key.

`POSTGRES_PASSWORD`: Password used to access the select postgres DB. The variable name can be changed in the config.yaml

`CRATE_PASSWORD`: Password used to access the select Crate DB. The variable name can be changed in the config.yaml

`DETAILS_LOCATION`: Location outside of Docker where the details .json files should be stored at. Defaults to `./data`

`CONFIG_FILE`: Name of the config file to be used *inside* Docker. Because the connection details may change compared to external
usage the file can be a different one from the default config.yaml. Defaults to `config_in_docker.yaml`

### Database
If you are using a dockerized database you can update the used network to be an external one then run both database and Lightshield
directly in the same network.


_The following docker-compose.yaml sample can be used as a starting point:_
```shell
docker-compose --profile all up -d
```

```yaml

version: '3.7'
services:

# Communication
  proxy:
    hostname: proxy
    image: doctress/spiritmight:dev
    environment:
      - ENVIRONMENT=riot_api_proxy
      - EXTRA_LENGTH=1.5
      - DEBUG=False
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - API_KEY=${API_KEY:?err}
      - INTERNAL_DELAY=2
    ports:
      - "127.0.0.1:8888:8888"
    profiles:
      - all
      - peripheral

  redis:
    hostname: redis
    image: redis:7.0-rc
    restart: always
    profiles:
      - all
      - peripheral

  rabbitmq:
    hostname: rabbitmq
    image: docker.io/bitnami/rabbitmq:3.10
    ports:
      - '127.0.0.1:4369:4369'
      - '127.0.0.1:5551:5551'
      - '127.0.0.1:5552:5552'
      - '127.0.0.1:5672:5672'
      - '127.0.0.1:25672:25672'
      - '127.0.0.1:15672:15672'
    environment:
      - RABBITMQ_LOGS=-
    volumes:
      - 'rabbitmq_data:/bitnami/rabbitmq/mnesia'
    profiles:
      - all
      - peripheral

### League Rankings

  league_ranking:
    hostname: league_ranking
    image: doctress/lightshield/league_ranking:latest
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - CRATE_PASSWORD=${CRATE_PASSWORD}
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    entrypoint: "poetry run lightshield run league_ranking"
    restart: always
    profiles:
      - league_ranking
      - all

### PUUID Collector

  puuid_collector:
    image: doctress/lightshield/puuid_collector:latest
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    entrypoint: "poetry run lightshield run puuid_collector"
    restart: always
    profiles:
      - puuid_collector
      - all

  puuid_collector_tasks:
    image: doctress/lightshield/puuid_collector_tasks:latest
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - CRATE_PASSWORD=${CRATE_PASSWORD}
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    entrypoint: "poetry run lightshield run puuid_collector.rabbitmq.tasks"
    restart: always
    profiles:
      - puuid_collector
      - all

  puuid_collector_results:
    image: doctress/lightshield/puuid_collector_results:latest
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - CRATE_PASSWORD=${CRATE_PASSWORD}
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    entrypoint: "poetry run lightshield run puuid_collector.rabbitmq.results"
    restart: always
    profiles:
      - puuid_collector
      - all

### Match History

  match_history:
    image: doctress/lightshield/match_history:latest
    hostname: match_history
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    entrypoint: "poetry run lightshield run match_history"
    restart: always
    profiles:
      - match_history
      - all

  match_history_tasks:
    image: doctress/lightshield/match_history_tasks:latest
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - CRATE_PASSWORD=${CRATE_PASSWORD}
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    entrypoint: "poetry run lightshield run match_history.rabbitmq.tasks"
    restart: always
    profiles:
      - match_history
      - all

  match_history_results:
    image: doctress/lightshield/match_history_results:latest
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - CRATE_PASSWORD=${CRATE_PASSWORD}
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    entrypoint: "poetry run lightshield run match_history.rabbitmq.results"
    restart: always
    profiles:
      - match_history
      - all

### Match Details

  match_details:
    image: doctress/lightshield/match_details:latest
    entrypoint: "poetry run lightshield run match_details"
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - CRATE_PASSWORD=${CRATE_PASSWORD}
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
      - type: bind
        source: ${DETAILS_LOCATION:-./data}
        target: /data
    restart: always
    profiles:
      - match_details
      - all

  match_details_tasks:
    image: doctress/lightshield/match_details_tasks:latest
    entrypoint: "poetry run lightshield run match_details.rabbitmq.tasks"
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - CRATE_PASSWORD=${CRATE_PASSWORD}
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    restart: always
    profiles:
      - match_details
      - all

  match_details_results:
    image: doctress/lightshield/match_details_results:latest
    entrypoint: "poetry run lightshield run match_details.rabbitmq.results"
    environment:
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - CRATE_PASSWORD=${CRATE_PASSWORD}
    volumes:
      - ./${CONFIG_FILE:-config_in_docker.yaml}:/project/config.yaml:ro
    restart: always
    profiles:
      - match_details
      - all

volumes:
  rabbitmq_data:
    driver: local

networks:
  default:
    name: lightshield

```
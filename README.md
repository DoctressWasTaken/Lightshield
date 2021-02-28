
# Lightshield (LS)

This tool provides a fully automated system to permanently mirror the official Riot APIs content
 with your local system. It uses a number of microservices to split requests done to different endpoints
 across separated processes and allows scaling each service according to your current data needs. 
 This is especially usefull in the early stages of setting up a data repository.

The setup is meant for tasks using alot of match data while not wanting to set up their own data pipeline structure.
Lightshield does currently not pull **all** match details data but only a select subset (for details
check the SQL schemas in the postgres folder). Changes can be made easily by simply expanding/replacing the service.

Lightshield is optimized to not repeat calls unecessarily. This comes at cost of having data added in a less than
real time fashion.
 
## Structure in Short

Lightshield handles data through a distributable scalable network of triangular microservice structures.
All data collected is stored in a dedicated postgres database. Task handling and scalability is provided through a 
buffering redis database.

- Each cluster of services is responsible for a single server and requires an additional proxy for rate limiting and 
handling of the API key.

Each step of data processing is stored inside the Postgres database from which a single manager service creates tasks.
Worker services then process the tasks, add data to the db and close the task.


## Requirements
Lightshield runs on docker and can either be built through this repository or by pulling the images 
[directly from DockerHub](https://hub.docker.com/u/lightshield).


## Setup

### I. Network

Initialize the network used to bundle all services together and allow communication: 
```shell script
docker network create lightshield
```
The name can be changed but has to be updated in the compose files as well.

### II. Database
Set up a postgres database either locally, in docker (attached to the network) or remotely. The services currently expect
no password verification as such using a remote postgres library should only be done if you can limit access through other means.

DB Connection details can be configured through a secrets.env file (template file included).

Lightshield requires the in the postgres folder listed tables to be set up in the specified database under schemas
corresponding to the server they will contain data for. E.g. pulling data for NA1 means setting up a schema `na1` (lower case)
with all tables inside said schema.

### III. Centralized Services
Services included in the `compose-global.yaml` file are centralized and as such do not run per-server.
This currently mainly includes the redis buffer database which should be started ahead of the other services.

### IV. Server Specific Structure

#### IV.0 Proxy
See the [Proxy Repository](https://github.com/LightshieldDotDev/Lightshield_proxy) for setup description.

#### For docker-compose
Run docker compose with default parameters. The services require the selected server to be passed into the container via
the environment variable `SERVER`. In addition make sure to use different project names either through `-p [project name]`
or through the env variable `COMPOSE_PROJECT_NAME`. This will stop multiple server setups from overwriting one another.

```shell script
# Build from source
docker-compose build
# Or pull from docker hub using TAG to specify the version you want
TAG=latest docker-compose pull
# Run either with the -p tag
SERVER=EUW1 docker-compose -p lightshield_euw1 up -d
# Or env variable
SERVER=EUW1 COMPOSE_PROJECT_NAME=lightshield_euw1 docker-compose up -d
```
#### For docker-swarm
Follow the same guidelines explained for docker-compose. The images can either be built or pulled from DockerHub.
`SERVER` still needs to be passed into the service.
The individual project name is passed through the stack name.
```shell script
SERVER=EUW1 docker stack deploy -c compose-services.yaml lightshield_euw1
```

<hr>

## Config
In addition to the configs listed in the secrets.env file there are default values used by docker in the .env file
as well as in the individual compose files (on the services) themselves.

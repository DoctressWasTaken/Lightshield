
# Lightshield (LS)

This tool provides a fully automated system to permanently mirror the official Riot APIs content
 with your local system. It uses a number of microservices to split requests done to different endpoints
 across separated processes.

The setup is meant for tasks using alot of match data while not wanting to set up their own data polling.

Lightshield is optimized to not repeat calls unecessarily. This comes at cost of having data added in a real time fashion.
*While unlikely services can drop singular datasets on restarts/shutdowns that will not be repeated unless manually forced.*
 

[Image Link]

## Structure in Short

Services rely on a standardized structure:
- Services take input from other services using a Websocket Client that connects to the publishing service.
- Services each rely on their own redis database for buffering of incoming as well as outgoing tasks. Buffer are limited
as not to build a major overhead at any point of the service chain.
- Services use a local SQLite database to hold details on already performed calls/ latest stats. This data is not 
kept in the Redis database as the in-memory cost would be too high.
- Services use a centralized Service Class/Worker Class System to perform calls to the API in an asynchronous manner.
The Service Class handles pulls and pushes to either buffer.
- All calls are issued through a centralized proxy service, tasked with ratelimiting the requests on client side. 
- Tasks are published to subscribed services using a websocket server. 

## Requirements
LS runs on docker-compose meaning that outside of the container system no data is needed.

## Setup

### I. API Key
Create a `secrets.env` file in the project folder and add your API key. 
```.env
API_KEY=RGAPI-xxx
```

### II. Base image & Network
Build the basic image used by all services:
```shell script
docker build -t lightshield_service ./services/base_image/
```
Initialize the network used: 
```shell script
docker network create lightshield
```

### III. Centralized Postgres
Calls and services are initiated on a **per server** basis, meaning that server can be added and removed
as interest in the data exists.

Data collected across **all** services is centralized in a final postgres database.
Settings for the postgres can be found in the `compose-persistent.yaml`.
Data is saved in the `data` database.\
Build and start the postgres db via:
```shell script
docker-compose -f compose-persistent.yaml build
docker-compose -f compose-persistent.yaml up -d
```

### IV. Server Specific Structure
The project uses every container multiple times. For this to work a different `COMPOSE_PROJECT_NAME` has 
to be set for each server. \
In adition the currently selected `SERVER` code has to be passed into the command to be passed on into
the services.\
**While the `COMPOSE_PROJECT_NAME` can be chosen and changed at will while running the project, the 
`SERVER` variable has to be kept consistent in spelling (always capital letters) as volumes used by 
the Redis databases, naming for the SQLite databases and server names in the postgres db all rely on this variable.** \
Initiate each server chain as follows:
```shell script
SERVER=EUW1 COMPOSE_PROJECT_NAME=lightshield_euw1 docker-compose build
SERVER=EUW1 COMPOSE_PROJECT_NAME=lightshield_euw1 docker-compose up -d
```
<hr>

## Config
Each service takes a number of required arguments:
#### Default
`SERVER` contains the selected api server. For the proper codes please refer to the riot api dev website.\
`WORKER` sets the number of parallel async worker started in the service.\
`MAX_TASK_BUFFER` sets the maximum number of incoming tasks buffered. *Outgoing tasks are currently not set via env variables.*\
`REQUIRED_SUBSCRIBER` [Optional] allows to set a number of services needed for the publisher to allow output. 
Only once all services mentioned are connected the publisher is allowed to broadcast tasks to all connected services.
*Service names are currently not set via environment variables.*

Services connecting to the postgres database can process host, port and user variables provided for the postgres db.
The parameters shown below are default parameters used in the services.
```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=db_worker
```

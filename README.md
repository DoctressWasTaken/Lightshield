## **There have been a bunch of changes not added to the readme yet.**
## Will be updated in the coming days, or just send me a message.



# Lightshield (LS)

* [Requirements](#requirements)
* [Setup](#setup)
  + [I. API Key](#i-api-key)
  + [II. Base Image and Network](#ii-base-image-and-network)
  + [III. Centralized Elements](#iii-centralized-elements)
  + [IV. Server Specific Structure](#iv-server-specific-structure)
  + [Container](#container)
    - [RabbitMQ](#rabbitmq)
    - [PostgreSQL](#postgresql)
    - [Proxy](#proxy)
    - [League Updater](#league-updater)
    - [Summoner ID Updater](#summoner-id-updater)
    - [Match History Updater](#match-history-updater)
    - [Match Updater](#match-updater)
    - [DB Worker](#db-worker)
* [Database-Structure](#database-structure)
  + [Summoners](#summoners)
  + [Matches](#matches)

This tool provides a fully automated system to permanently mirror the official Riot APIs content
 with your local system. It uses a number of Microservices to pull currently ranked players as well
 as all matches on the server into a local database and keeps it updated.
 
This is an ideal setup for someone interested only in frontend or data analysis work not wanting to 
deal with setting up routines and regular calls to the api nor caching of results.

LS is optimized to not repeat calls unless necessary. All data is made available in a final postgres
database using only a single table style for both match and summoner details. 

**This system is not structured to return realtime or necessarily 100% complete content. (yet)**
- Matchhistories are, by default, only updated after 10 new matches played for efficiency reasons (Its likely that someone else that has played in that match will trigger a pull anyway).
- There is a minor issue that matches, after being called but before being added to the DB, will not be repeated if the service is killed during that time. While it should properly flush the data this is not implemented fully reliable yes.
- Its currently only pulling soloQ matches, It's currently also only pulling match-details (not timelines)

## Requirements
LS runs on docker-compose meaning that outside of the container system no data is needed.

## Setup

### I. API Key
Create a `secrets.env` file and add your API key. 
```.env
API_KEY=RGAPI-xxx
```

### II. Base image & Network
3 services are based on a centralized base image included in the project. It has to be created 
locally:
```shell script
docker build -t lightshield_service ./services/base_image/
```
All services are connected to an external network. It has to be created before running the container with:
```shell script
docker network create lightshield
```

### III. Centralized Elements
Core of the system are the RabbitMQ messaging system to provide fail-proof communication between 
services as well as the postgreSQL database holding the final output.
Those services are contained in the `compose-persistent.yaml` file. Build and start both via:
```shell script
docker-compose -f compose-persistent.yaml build
docker-compose -f compose-persistent.yaml up -d
```

### IV. Server Specific Structure
For each server scraped an individual chain of container has to be set up. They run on their own using only the central rabbitmq messaging queue service as well as the final postgresql server unitedly.

Because docker-compose, by itself, wont let you run multiple instances of a container side by side (outside of scaling) the project has to be initiated with different `COMPOSE_PROJECT_NAME` values.

In addition the server code has to be provided. It is used for generating the proper requests to the api, naming messaging queues and selecting/creating the appropriated volumes for the temporary databases.

```shell script
SERVER=EUW1 COMPOSE_PROJECT_NAME=lightshield_euw1 docker-compose build
SERVER=EUW1 COMPOSE_PROJECT_NAME=lightshield_euw1 docker-compose up -d
```
The `SERVER` value has to contain a legit server code used by the riot API.

**While the` COMPOSE_PROJECT_NAME` can contain any name desired and can be changed by shutting down and restarting the services, the volumes containing info on what elements have already been cached are linked to the `SERVER` value, meaning that this value can not change.** 
Probably best to just use `lightshield_` + the current server.
 
<hr>

 ### Container
 
Explanation to the different container and their config options in the .env file.
 
#### RabbitMQ
Messaging Queue service that handles all communication between the microservices allowing them to be shut down/started individually if needed.
Queues are persistent, however only messages between the match_history_updater and the match_updater service are durable as they are created only a limited times (*10 times, 1 for each player that has the match in his matchhistory*)
All services are limited to not pause once a queue size climbs above 2k messages to avoid overly overloading rabbitmq.

#### PostgreSQL
Postgres Database containing the finalized data on players and matches.
This database is not set up for massive queries and should be used as a persistent buffer. 
An option can be to export and purge matches from this database on a patch-cycle basis to move them into individual datapools.
The data structure is shown [below](#database-structure).
 
#### Proxy
The proxy manages rate limits across all services of the server-chain. 
It is set up fairly conservatively prioritizing safety over *perfect* usage of the limit.

#### League Updater
Using the **league-exp** / **league** endpoint this services periodically scrapes all ranked user
by division.
A small redis database is running along the process to not pass on any summoner that do not have new matches played.

```
UPDATE_INTERVAL    Describes the minimum time between scrape cycles for the application.
                   Going below 2-3 hours is not recommended nor will it change the speed at which
                   the system works due to the number of calls/api limits that have to be executed.
                   The initial setup should be set to multiple days to allow the following 
                   services to initially register all user.

LEAGUE_BUFFER      Describes the maximum parallel calls made to the API. !Consider the API method limit.
```

#### Summoner ID Updater
Using the summoner endpoint this services pulls user IDs from the API. This is done only once for each user
creating a large overhead when first starting the download but falling off over time.
A small redis database is running along the process to track already requested IDs.

```
SUMMONER_ID_BUFFER Describes the maximum parallel calls made to the API. !Consider the API method limit.``
```
 
#### Match History Updater
Using the match endpoint this service pulls match histories from the API. This is done only after the player has 
played a specified minimum number of new matches compared to the last update. As each call contains 100
match Ids, multiple calls per user can be required but contain at least 100 matches that are passed on. 
```
MATCH_HISTORY_BUFFER   Describes the maximum parallel 'user' that are processed. Each user can 
                        require multiple calls (especially when starting the system). This value
                        can be increased, once the initial data is pulled.
 
MATCHES_TO_UPDATE      Describes the minimum new matches required to trigger the update of a user.
                        10 being default means that on average 1 call per match is made
                        (10 calls, one for each participant).
                        
TIME_LIMIT             Oldest timestamp (in seconds) to be pulled.
```

#### Match Updater
Using the match endpoint this service pulls match details from the API. Match details are only pulled once.
Matches pulled are marked in the local redis database and not updated again. Matches pulled are directly added to the postgres database.
 
```
MATCH_BUFFER Describes the maximum parallel calls made to the API. !Consider the API method limit.
```

#### DB Worker
The DB Worker takes datasets passed on from the Summoner ID Updater service and writes them into the postgres database.

```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=db_worker
```
To be changed in case an already existing postgres container or external postgres DB was to be used.

### Database-Structure
All data is saved in a Postgres Database `data`. The postgres container, by default, has its internal port mapped to localhost:5432. In case another postgres db is running locally this has to be changed or the container wont start.

Data can be accessed through psql or similar means from outside.

#### Summoners
Summoner are only inserted once and not updated. A new insert is triggered once the summoner is passed through the summoner_id_updater service.
*(While shown in camelCase below all column names are lower letters only)*
```python
    summonerId = Column(String(50))
    accountId = Column(String(56))
    puuid = Column(String(78), primary_key=True)

    summonerName = Column(String)
    tier = Column(Enum(Tier))
    rank = Column(Enum(Rank))

    series = Column(String(4))

    wins = Column(Integer)
    losses = Column(Integer)

    server = Column(Enum(Server))
```

#### Matches
A new insert is triggered once the match is pulled by the match_updater service.
*(While shown in camelCase below all column names are lower letters only)*
sub-tables that are usually present in the match details are saved as json values and cramped into a JSON field. 
```python
    matchId = Column(BigInteger, primary_key=True)
    queue = Column(Integer)
    gameDuration = Column(Integer)
    server = Column(Enum(Server), primary_key=True)
    gameCreation = Column(BigInteger)
    seasonId = Column(Integer)
    gameVersion = Column(String(20))
    mapId = Column(Integer)
    gameMode = Column(String(15))

    participantIdentities = Column(JSON)
    teams = Column(JSON)
    participants = Column(JSON)
```

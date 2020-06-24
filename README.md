### Just wan't to mention that while it does work mostly it still has a bunch of bugs and changes (see Issues) that will break the current data on update.

# Lightshield (LS)


 + [Centralized Elements](#centralized-elements)
   - [Created Container](#created-container)
 + [Server Specific Elements](#server-specific-elements)
   - [Created Container](#created-container-1)
     * [Proxy](#proxy)
     * [League Updater](#league-updater)

This tool provides a fully automated system to permanently mirror the official Riot APIs content
 with your local system. It uses a number of Microservices to pull currently ranked players as well
 as all matches on the server into a local database and keeps it updated.
 
This is an ideal setup for someone interested only in frontend or data analysis work not wanting to 
deal with setting up routines and regular calls to the api nor caching of results.

LS is optimized to not repeat calls unless necessary. All data is made available in a final postgres
database using [tolkis] format. 

A number of changes can be made towards performance and speed at which the system updates.

##Requirements
LS runs on docker-compose meaning that outside of the container system no data is needed.

##Setup

### I. API Key
Create a `secrets.env` file and add your API key. 
```.env
API_KEY=RGAPI-xxx
```

### II. Centralized Elements
Core of the system are the RabbitMQ messaging system to provide fail-proof communication between 
services as well as the postgreSQL database holding the final output.
Those services are contained in the `compose-persistent.yaml` file. Build and start both via:
```shell script
docker-compose -f compose-persistent.yaml build
docker-compose -f compose-persistent.yaml up -d
```
The build instructions used in the postgres container contain the initial setup of the relevant tables.

#### Created Container

#####RabbitMQ
Messaging Queue service that handles all communication between the microservices allowing them to scale
and be shut down/started individually if needed. 

#####PostgreSQL
Postgres Database containing the finalized data on players and matches.

<hr>

### III. Server Specific Elements
All communication is done via the same instance of rabbitMQ and postgreSQL. The services themselves
are however split by server, meaning that for each server you want to pull data from (eg. NA, EU, KR)
an extra chain of container has to be started.

To properly do this 2 environment variables have to be declared before running the command
```shell script
SERVER=EUW1 COMPOSE_PROJECT_NAME=lightshield_euw1 docker-compose build
SERVER=EUW1 COMPOSE_PROJECT_NAME=lightshield_euw1 docker-compose up -d
```
The `SERVER` value has to contain a legit server code used by the riot API.
The `COMPOSE_PROJECT_NAME` should contain a name different to the name initially used for the creation
of the centralized elements (which defaults to `lightshield`) to avoid issues. Recommended is to just
add the current server id to the end.

 #### Created Container
 
 ##### Proxy
 The proxy manages rate limits across all services of the server-chain. 
 ##### League Updater
 Using the **league-exp** / **league** endpoint this services periodically scrapes all ranked user
 by division. Following settings can be changed via the .env file:
 
 `UPDATE_INTERVAL (default=48)` describes the minimum time between scrape cycles for the application.
 Going below 2-3 hours is not recommended nor will it change the speed at which the system works due
 to the number of calls/api limits that have to be executed. The initial setup should be set to multiple
 days to allow the following services to initially register all user.
 
 `LEAGUE_BUFFER (default=5)` describes the maximum parallel calls made to the API. **Consider the API method limit.**
 
 #####Summoner ID Updater / Redis Database
 Using the summoner endpoint this services pulls user IDs from the API. This is done only once for each user
 creating a large overhead when first starting the download but falling off over time.
 A small redis database is running along the process to track already requested IDs.
 Following settings can be changed via the .env file:
 
 `SUMMONER_ID_BUFFER (default=30)` describes the maximum parallel calls made to the API. **Consider the API method limit.**
 
 #####Match History Updater / Redis Database
 Using the match endpoint this service pulls match histories from the API. This is done only after the player has 
 played a specified minimum number of new matches compared to the last update. As each call contains 100
 match Ids, multiple calls per user can be required but contain at least 100 matches that are passed on.
 Following settings can be changed via the .env file: 
 
 `MATCH_HISTORY_BUFFER (default=4)` describes the maximum parallel **user** that are processed. Each user
 can require multiple calls (especially when starting the system). This value can be increased, once the initial 
 data is pulled.
 
 `MATCHES_TO_UPDATE (default=10)` describes the minimum new matches required to trigger the update of a user.
 10 being default means that on average 1 call per match is made (10 calls, one for each participant).
 
 #####Match Updater / Redis Database 
 Using the match endpoint this service pulls match details from the API. Match details are only pulled once.
 Matches pulled are marked in the local redis database and not updated again. Following settings can be changed 
 via the .env file:
 
 `MATCH_BUFFER (default=30)` describes the maximum parallel calls made to the API. **Consider the API method limit.**
 
 #####DB Worker
 The DB Worker takes datasets passed on from the Summoner ID Updater service as well as the Match Updater
service and writes them into the postgres database. Following settings can be changed via the .env file:

`POSTGRES_HOST=postgres` 

`POSTGRES_PORT=5432`

`POSTGRES_USER=db_worker`

To be changed in case an already existing postgres container or external postgres DB was to be used.

**If changed the db and table structure has to be set up manually!**

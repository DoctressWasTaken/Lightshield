
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
 

## What Lightshield does **not** do well
#### *Deliver up to date data*  
While it can get up to date data on user its not set up for this by default

#### *Gather data on unranked player*  
By default Lightshield pulls data through the league endpoint which requires a user to be ranked.
As such getting information over unranked player is not supported by default but can be manually added.

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

### Env variables
Copy and rename the included secrets_template.env into secrets.env

### I. Network

Initialize the network used to bundle all services together and allow communication: 
```shell script
docker network create lightshield
```
The name can be changed but has to be updated in the compose files as well.
If you are planning on running it through docker swarm use the appropriate network type.

### II. Database
Set up a postgres database either locally, in docker (attached to the network) or remotely. The services currently expect
no password verification as such using a remote postgres library should only be done if you can limit access through other means.

DB Connection details can be configured through a secrets.env file (template file included).

Lightshield requires the in the postgres/ folder listed tables to be set up in the specified database under schemas
corresponding to the server they will contain data for. E.g. pulling data for NA1 requires setting up a schema `na1` (lower case)
with all tables inside said schema as well as a user `na1` which will be used to pull data from said schema.

### III. Centralized Services
Services included in the `compose-global.yaml` file are meant to be run centralized meaning they run server-agnostic (usually as a one-off).
Currently this refers to the proxy service as well as the buffer redis db.
Start the services by properly refering to the compose file:
```shell
# Pull (specify a tag if needed, defaults to latest)
TAG=latest docker-compose -f compose-global.yaml pull
# Or build
docker-compose -f compose-global.yaml build
# And then run
docker-compose -f compose-global.yaml up -d
```

### IV. Server Specific Structure

#### For docker-compose
Run docker compose with default parameters. The services require the selected server to be passed into the container via
the environment variable `SERVER`. In addition make sure to use different project names either through `-p [project name]`
or through the env variable `COMPOSE_PROJECT_NAME`. This will stop multiple server setups from overwriting one another.

```shell script
# Build from source
docker-compose build
# Or pull from docker hub (specify a tag if needed, defaults to latest)
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
The compose file includes the base_image service which is just a unified default image for all other services. As such
this isn't an actual runnable service and would usually just immediatly exit when run in docker-compose. Swarm however tries
to rebuild and restart the service continuously as such you need to manually remove it from the stack to avoid that.

<hr>

## Functionality

Services are structured in general in a triangular form using the persistent postgres DB as source of truth,
a singular non-scalable manager to select new tasks and a scalable amount of microservices to work through those tasks.

2 lists are used to buffer tasks:
- A list of non-selected tasks that are yet to be worked on.
- A list of selected tasks that are being or have been work on  
The manager service looks at both lists to determine if it should add a new task to the first non-selected task list.
  Each worker service pulls tasks from the first list and adds them to the secondary list. Tasks are never marked as done
  as the tasks are by default no longer eligible once inserted in the DB, e.g. if a summonerId has to be pulled then the manager
  will only select accounts without summonerId as tasks. Once the summonerId is added it will automatically be ineligible.
  All tasks that are older than `TASK_BLOCKING` (secrets.env) minutes in the secondary selected list are periodically removed by the manager
  therefore making space for new tasks. As this is the only way tasks are being removed make sure to keep the limit just high enough
  to assure that tasks currently being worked on are not removed while not having the queue overflow with already done tasks.
  
### Services

#### League Ranking
Uses the league-exp endpoint to crawl all ranked user in a continuous circle. This service has no manager and only requires a one-off.
via the `UPDATE_INTERVAL=1` variable in the compose file you can limit the delay between cycles in hours. By default after finishing
with challenger the service will wait 1 hour to restart on Iron games.

### Summoner ID
Using the summoner endpoint pulls the remaining IDs for each account that were not included in the league ranking response.
This is a one time tasks for each user but it will take alot of initial requests until all are done.

### Match History
Pull and update the match history of user. Prioritizes user for which no calls have been made so far and, once all user have
had their history pulled user that have had the most soloQ matches played since their last update.
Use the `MIN_MATCHES=20` variable to limit how many new matches a player has to play to even be considered for an update.
Because for each match 10 match-history are updated one should consider keeping this number high as to not make 10 match history
calls per new match. Setting it to 10 or 20 means that for each match played on average only 1 or .5 calls have to be made.

### Match Details
Pull match details for a buffered match_id. This service pulls match details and adds a select number of attributes to the DB 
(check the SQL files for more info). If more/less data is needed you have to update the service + db schema accordingly.
Matches are pulled newest to oldest and are not pulled  for matches older than defined through `DETAILS_CUTOFF` (secrets.env).

### Match History
Currently not implemented, WIP.


## Lightshield Tools

Tools and Code-Sections of the Lightshield Framework that were better fit to be provided through dependency
rather then included in the main project.

#### What currently doesn't work:
- The keys used to save data in Redis are not linked to the API key, as such multiple keys have to use
multiple Redis servers.

### Ratelimiter (WIP)

Multi-Host async ratelimiting service. The clients each sync via a central redis server. 

Set up the proxy in an async context with redis connection details.
```python
from lightshield.proxy import Proxy
import aiohttp

async def run():
    p = Proxy()
    # Initiate the redis connector in async context
    await p.init(host='localhost', port=5432)
```

Make calls directly to one endpoint.
Provide the server the calls are run against and an identifier that helps you recognize which endpoint the requests are
run against
```python
async with aiohttp.ClientSession(headers={'X-Riot-Token': ''}) as session:
    zone = await p.get_endpoint(server='europe', zone='league-exp')
    for page in range(1, 10):
        zone.request('https://euw1.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/SILVER/I?page=%s' % page, session)
```

### Settings (WIP)
The settings file contains a number of variables that are used across the project.
Variables can be set through:  
`ENV > config.json > default`
```python
from lightshield import settings

headers = {'X-Riot-Token': settings.API_KEY}
```

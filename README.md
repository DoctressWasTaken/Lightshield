
# Lightshield

A Self-Contained Pipeline to keep a local mirror of the RiotGames API.  
Does:
- Pull regular updates on players ranks
- Update players match histories
- Save match_details and match_timeline for matches

Does not:
- Give real time updates on players
- Give per match updates on players ranks
- Work well with personal key ratelimits

Lightshield is optimized to not repeat calls unecessarily. This comes at cost of having data added in a less than
real time fashion.
 
### Standalone Multi-Host Ratelimiter 
Lightshield offers its ratelimiter as a standalone [python library](https://pypi.org/project/lightshield/) which only
requires the redis services included in this larger package.

For more details see [here.](Rate%20Limiting.md)



## Setup
Lightshield runs almost fully functionally out of the box simply starting the docker-compose file. 
- No services will start by default they have to be turned on in the included interface manually.
- *Match Details* and *Match Timeline* JSON files will by default be saved in the `match_data/` folder located in the 
project folder itself. If the folder doesn't exist, it has to be created.
- Lightshield does by default not run with any exposed ports outwards, all port exposure is localhost only. As such 
accessing the web-interface is only possible if the service is started locally or through ssh tunneling. For ssh tunneling
  the following ports should be bound: `ssh -L 8301:localhost:8301 -L 8302:localhost:8302`. This will enable communication 
  to both the front and backend services and the interface will therefore be reachable locally under `localhost:8301`.
  

### Data Storage
#### Postgres
Data is by default stored in the `lightshield` database in the included postgres service. Data is partially saved in
platform-wide schemas (e.g. `EUW1. | NA1.`), centrally, or in region-wide schemas (e.g. `europe. | americas.`).
Details on structure can be found in the corresponding [folder.](postgres)
#### JSON Files
Both Details and Timeline files are saved locally instead of the DB. This is to reduce load on the DB overall as well as
speed up services to not require the inserting of neither large JSON blobs nor multiple normalized table entries per match.
Only identifying parameters are stored in the DB.  
The following folder structure is used to save data locally for JSON files:
##### Match Details
`match_data/details/[patch]/[day]/[platform]/[matchid].json`
##### Match Timeline
`match_data/timeline/[platform]/[match_id[:5]]/[matchid].json`

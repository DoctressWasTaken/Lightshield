# _config.yaml_

The file can be generated by using `lightshield init-config`. 
If you are planning on running services in different setups (e.g. locally and in docker) consider creating different
versions with different connection details, one using the expected `config.yaml` name for local execution and another
to be bound and renamed into the container.


## Version

A version number used by the library to determine what content
to expect

## Connections

Connection details for inter- or external connections.

#### Postgres
The password cannot be inserted directly but must be provided either
via an environmental variable or via a `.env` file.

```yaml
    postgres:
      hostname: [default: 'localhost']
      port: [default: 5432]
      user: [default:' postgres']
      database: [default: 'lightshield']
      # Password will be pulled from the provided ENV variable.
      # No value found assumes no password needed
      password_env: [default: 'POSTGRES_PASSWORD']
```
#### Proxy
The proxy seems to not allow cross-protocol requests (something breaks internally, feel free to submit a pull request),
therefore the request protocol needs to equal the proxy protocol.  
- If the proxy is reachable via localhost or IP -> *http*
- If the proxy is reachable via an externally hosted domain behind a reverse proxy -> *https*
```yaml
    proxy:
      location: [default: 'localhost:8888']
      protocol: [default: 'http']
```

## Services

#### Platform and Rank
By default all possible values are selected to be worked through by the services. However a filtered include list can be 
provided
```yaml
platforms:
  - EUW1
  - KR
```
If both are provided the exclude list will be ignored.


### League Ranking
```yaml
    league_ranking:
      # Timeout before restarting the entire ranking spectrum in hours. Timeout starts the moment any division is done.
      cycle_length: [default: 6]
      platforms:
      ranks:
```
### PUUID Collector
No configs atm

```yaml
    puuid_collector:
```

### Summoner Tracker
```yaml
    summoner_tracker:
      # Min days before a new summoner-v4 request is started to check for updates
      # Because the match-history updates are based upon an updated activity value provided by this service the updates
      # should not be too infrequent.
      update_interval: [default: 1]
      # Days of last activity before the first server-swap check is made.
      first_swap_check: [default: 13]
      # After the first check the interval for server-swap checks is reduced.
      swap_check_interval: [default: 28]
```

### Match_History
```yaml
    match_history:
      # Limits to how far back updates should run whichever arrives quicker
      history:
        # By days
        days: [default: 14]
        # By matches
        matches: [default: 100]
      # Can be provided a single value to filter it to that exact queue [No multi-selection possible]
      queue:
      # Can be provided a single value to filter. Options: [ranked, normal, tourney, tutorial] 
      types: [default: 'ranked']
      # See platform filter
      platforms:
```
See https://static.developer.riotgames.com/docs/lol/queues.json for possible queues.


## Statics
**This section should not be changed unless due to API changes.**  

The statics section is used by services to pull basic context from.  
If you want to globally ignore a platform or rank you can delete it from the list,  
however be aware that filters might no longer work or even lead to exceptions.
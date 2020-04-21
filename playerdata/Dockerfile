FROM postgres:alpine
COPY playerdata/db_init.sql /docker-entrypoint-initdb.d/
COPY playerdata/player_euw.sql /docker-entrypoint-initdb.d/
COPY playerdata/player_kr.sql /docker-entrypoint-initdb.d/
COPY playerdata/player_na.sql /docker-entrypoint-initdb.d/

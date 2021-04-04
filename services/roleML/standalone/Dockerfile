ARG TAG
FROM lightshield/base_image:$TAG
RUN apt-get update
RUN apt-get install -y curl
# Main Application
COPY queues.json .
RUN curl -o queues.json http://static.developer.riotgames.com/docs/lol/queues.json
COPY *.py ./


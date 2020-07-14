FROM lightshield_service

WORKDIR /project
COPY startup.sh .
RUN chmod 500 startup.sh

COPY tables/ tables/
COPY *.py ./

CMD . ./startup.sh

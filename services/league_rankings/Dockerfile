FROM lightshield_service

WORKDIR /project
COPY startup.sh .
RUN chmod 500 startup.sh

# Main Application
COPY *.py ./

CMD . ./startup.sh

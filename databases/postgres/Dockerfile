FROM postgres:alpine
COPY *.sql /docker-entrypoint-initdb.d/
COPY wait-for-it.sh /project/
RUN chmod 500 /project/wait-for-it.sh

FROM redis

COPY redis.conf /usr/local/etc/redis/redis.conf
COPY wait-for-it.sh /project/
RUN chmod 500 /project/wait-for-it.sh

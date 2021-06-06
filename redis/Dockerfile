ARG LOG_LEVEL=INFO
FROM redis:6.2

WORKDIR /project
COPY *.sh ./
COPY lua_scripts/ lua_scripts/
RUN chmod 500 *.sh

CMD ["/project/startup.sh"]

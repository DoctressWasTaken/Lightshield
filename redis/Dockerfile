ARG LOG_LEVEL=INFO
FROM redis:7.0-rc

WORKDIR /project
COPY *.sh ./
COPY lua_scripts/ lua_scripts/
RUN chmod 500 *.sh

CMD ["/project/startup.sh"]

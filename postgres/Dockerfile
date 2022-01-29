FROM postgres:14.1

WORKDIR /docker-entrypoint-initdb.d
COPY startup_scripts/ .
COPY startup_script_templates/ startup_script_templates/
COPY setup.sh .
RUN chmod 500 *.sh
RUN ./setup.sh
RUN rm -r startup_script_templates/
RUN rm setup.sh
RUN ls -la

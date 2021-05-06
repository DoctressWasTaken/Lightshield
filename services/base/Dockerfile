FROM python:3.8-slim

WORKDIR /project
COPY *.sh ./
RUN chmod 500 *.sh
RUN pip install poetry
COPY poetry.lock .
COPY pyproject.toml .
RUN poetry install

COPY exceptions.py .
COPY settings.py .
COPY connection_manager/ connection_manager/

CMD ["/project/startup.sh"]

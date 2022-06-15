FROM python:3.9-slim

WORKDIR /project
RUN pip install poetry
COPY poetry.lock .
COPY pyproject.toml .
COPY *.sh ./
RUN chmod 500 *.sh
RUN poetry install
COPY endpoints.yaml .
COPY scripts/* scripts/
COPY *.py ./

CMD ["/project/startup.sh"]


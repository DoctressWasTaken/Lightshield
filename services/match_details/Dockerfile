FROM python:3.9-slim

WORKDIR /project
COPY *.sh ./
RUN mkdir config
RUN chmod 500 *.sh
RUN pip install poetry
COPY poetry.lock .
COPY pyproject.toml .
RUN poetry install

COPY *.py ./

CMD ["/project/startup.sh"]

FROM python:3.9

WORKDIR /project

COPY ./service/ /project/
RUN pip install poetry
RUN poetry install


CMD ["poetry", "run", "python", "-u", "-m", "http.server", "8000"]

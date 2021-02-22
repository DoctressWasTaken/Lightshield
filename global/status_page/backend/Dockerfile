FROM python:3.8-slim

WORKDIR /project
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py ./

#ENTRYPOINT ["/project/startup.sh"]
CMD ["gunicorn", "run:start_gunicorn", "--worker-class", "aiohttp.worker.GunicornWebWorker",  "--log-level", "info", "--bind", "0.0.0.0:8000"]

FROM python:3.7

WORKDIR /project
RUN mkdir logs
RUN python -m pip install --upgrade pip

COPY requirements.txt .
RUN  pip install -r requirements.txt

COPY limits.json .
COPY *.py ./

CMD ["python", "-u", "run.py"]

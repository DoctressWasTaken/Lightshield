FROM python:3.8

WORKDIR /project
RUN python -m pip install --upgrade pip

COPY wait-for-it.sh .
RUN chmod 500 /project/wait-for-it.sh
COPY requirements.txt .
RUN  pip install -r requirements.txt

COPY limits.json .
COPY *.py ./

CMD ["python", "-u", "run.py"]

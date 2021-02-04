FROM python:3.8

WORKDIR /project
COPY *.sh .
RUN chmod 500 *.sh
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py ./

CMD . ./startup.sh

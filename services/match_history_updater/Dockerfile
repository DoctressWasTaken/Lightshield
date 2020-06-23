FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip
COPY wait-for-it.sh .
COPY startup.sh .
RUN chmod 500 wait-for-it.sh startup.sh

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py ./

CMD . ./startup.sh

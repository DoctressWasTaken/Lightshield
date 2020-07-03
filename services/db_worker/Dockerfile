FROM python:3.8-slim

WORKDIR /project
RUN python -m pip install --upgrade pip
COPY wait-for-it.sh .
COPY startup.sh .
RUN chmod 500 wait-for-it.sh startup.sh

# Main Application setup
COPY requirements.txt .
RUN pip install -r requirements.txt
# Main Application
COPY tables/ tables/
COPY *.py ./

CMD . ./startup.sh

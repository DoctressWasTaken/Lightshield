FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip
COPY wait-for-it.sh .
# Main Application setup
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
# Main Application
COPY startup.sh .
COPY run.py . 

RUN chmod 500 startup.sh

CMD ["/project/startup.sh"]

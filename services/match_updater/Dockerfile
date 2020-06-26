FROM lightshield_service

WORKDIR /project
COPY startup.sh .
RUN chmod 500 startup.sh

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py ./

CMD . ./startup.sh

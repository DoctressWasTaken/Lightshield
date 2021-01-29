FROM python3.8

WORKDIR /project
COPY startup.sh .
COPY wait-for-it.sh .
RUN chmod 500 wait-for-it.sh startup.sh


COPY  requirements.txt .
RUN pip install -r requirements.txt

COPY ./lol_dto lol_dto/
COPY *.py ./
CMD . ./startup.sh

FROM lightshield_service

WORKDIR /project
COPY startup.sh .
RUN chmod 500 wait-for-it.sh startup.sh

# Main Application setup
COPY requirements.txt .
RUN pip install -r requirements.txt
# Main Application
COPY tables/ tables/
COPY *.py ./

CMD . ./startup.sh

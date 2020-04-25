FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip
# Main Application
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY startup.sh .
COPY run.py .

# Data module
RUN chmod 500 startup.sh

CMD ["/project/startup.sh"]

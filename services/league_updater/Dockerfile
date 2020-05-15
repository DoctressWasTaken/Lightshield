FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip
COPY wait-for-it.sh .
RUN chmod 500 wait-for-it.sh

# Main Application setup
COPY requirements.txt .
RUN pip install -r requirements.txt
# Main Application
COPY run.py .

CMD ["python", "-u", "run.py"]

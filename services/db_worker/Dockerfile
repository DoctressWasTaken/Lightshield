FROM python:3.7-slim

WORKDIR /project
RUN python -m pip install --upgrade pip

# Main Application setup
COPY requirements.txt .
RUN pip install -r requirements.txt
# Main Application
COPY run.py .

RUN ls

CMD python -u run.py
#CMD ["python", "-u", "run.py"]

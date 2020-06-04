FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip
# Main Application
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY *.py ./

CMD ["python", "-u", "run_async.py"]

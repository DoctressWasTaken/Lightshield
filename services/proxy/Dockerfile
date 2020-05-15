FROM python:3.7

WORKDIR /project
RUN python -m pip install --upgrade pip

COPY wait-for-it.sh .
COPY requirements.txt .
RUN  pip install -r requirements.txt

COPY limits.json .
COPY run.py .

CMD ["python", "-u", "run.py"]

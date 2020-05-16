FROM python:3.7


WORKDIR /project
RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY *.py ./
COPY server/ server/
COPY container/ container/

CMD ["python", "-u", "run.py"]

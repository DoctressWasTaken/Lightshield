FROM python:3.7


WORKDIR /project
RUN python -m pip install --upgrade pip
COPY requirements.txt .
COPY container.yaml .
RUN pip install -r requirements.txt
COPY run.py .

CMD ["python", "-u", "run.py"]

FROM python:3.8-slim

WORKDIR /project
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py ./

#ENTRYPOINT ["/project/startup.sh"]
CMD ["python", "-u", "run.py"]

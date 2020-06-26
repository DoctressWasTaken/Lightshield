FROM python:3.8

WORKDIR /project
RUN python -m pip install --upgrade pip
COPY wait-for-it.sh .
RUN chmod 500 wait-for-it.sh

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY *.py ./

CMD ["/"]
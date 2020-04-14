FROM python:3.7

WORKDIR /project
RUN python -m pip install --update pip
COPY api_component/requirements.txt api_component/requirements.txt
RUN pip install -r api_component/requirements.txt
COPY config.json .
COPY api_component/ .

CMD ['python', 'api_component/run.py']

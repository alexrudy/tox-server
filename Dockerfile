FROM python:3.7

COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
RUN mkdir -p /src
COPY . /src/
RUN pip install -e /src/

RUN mkdir -p /app

WORKDIR /app
COPY ./example/ /app/

EXPOSE 7654
ENV TOX_SERVER_PORT=7654
EXPOSE 7655
ENV TOX_SERVER_STREAM_PORT=7655

ENV TOX_SERVER_BIND_HOST='0.0.0.0'

ENTRYPOINT ["tox-server", "serve"]

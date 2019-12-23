FROM python3:latest

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r requirements.txt
COPY tox_server.py /usr/bin/tox-server
RUN chmod +x /usr/bin/tox-server

RUN mkdir -p /app

WORKDIR /app

EXPOSE 7654
EXPOSE 7655
ENTRYPOINT ["python3", "/usr/bin/tox-server", "serve"]

# Tox Server

[![PyPI version](https://badge.fury.io/py/tox-server.svg)](https://badge.fury.io/py/tox-server)
![Tests](https://github.com/alexrudy/tox-server/workflows/Tox%20Server%20Tests/badge.svg)

`tox-server` is a command line tool which runs [tox](https://tox.readthedocs.io/en/latest/) in a loop
and calls it with commands from a remote CLI. It responds to commands
via [ZeroMQ](https://zeromq.org). It isn't super useful on its own (as it doesn't eliminate the startup time for tox, just runs it repeatedly) but it is
helpful if your tests have to be run inside another environment with some setup cost, such as a docker container.

## Installation

You can use `pip` to install `tox-server`:

```
$ pip install tox-server
```

## Run the server

On the remote host:

```
$ tox-server serve
```

The server binds to `127.0.0.1` (i.e. it will only accept connections from localhost) because it is
not secured. If you are running it on an isolated network (like via docker), you can bind it to another host
with `tox-server  -b 0.0.0.0 serve`.

## Run a tox command remotely

On your local host, you can run a tox command:

```
$ tox-server run -e py37
```

This will run `tox -e py37` on the remote host.

## A note on security

Basically, security is hard. This program doesn't provide any authentication mechanism, and I'm not tempted
to add one. Before you expose ports from `tox-server` to the world wide web, I'd advise using something like
SSH tunneling to provide security and authentication.

# Tox Server

`tox-server` is a command line tool which runs [tox][] in a loop
and calls it with commands from a remote CLI. It responds to commands
via [ZeroMQ][]. It isn't super useful on its own (as it doesn't eliminate the startup time for tox, just runs it repeatedly) but it is
helpful if your tests have to be run inside another environment with some setup cost, such as a docker container.

To illustrate this, a minimal docker file is included.
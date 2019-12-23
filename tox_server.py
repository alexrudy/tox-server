#!/usr/bin/env python3
import os
import selectors
import subprocess
import sys
import time
from typing import cast
from typing import Dict
from typing import IO
from typing import List
from typing import Tuple

import click
import zmq


@click.group()
@click.option("-p", "--port", type=int, envvar="TOX_SERVER_PORT", help="Port to connect for tox-server", required=True)
@click.option(
    "--stream-port", type=int, envvar="TOX_SERVER_STREAM_PORT", help="Port to connect for tox-server stream output"
)
@click.option(
    "-h", "--host", type=str, envvar="TOX_SERVER_HOST", default="localhost", help="Host to connect for tox-server",
)
@click.pass_context
def main(ctx: click.Context, host: str, port: int, stream_port: int) -> None:
    """Interact with a tox server."""
    cfg = ctx.ensure_object(dict)
    cfg["host"] = host
    cfg["port"] = port
    cfg["stream_port"] = stream_port


def client(host: str, port: int) -> zmq.Socket:
    zctx = zmq.Context.instance()
    socket = zctx.socket(zmq.REQ)
    socket.connect(f"tcp://{host:s}:{port:d}")
    socket.set(zmq.LINGER, 10)
    return socket


def output_streams() -> Dict[bytes, IO[bytes]]:
    return {b"STDERR": getattr(sys.stderr, "buffer", sys.stderr), b"STDOUT": getattr(sys.stdout, "buffer", sys.stdout)}


def run_tox_command(command: List[str], output: zmq.Socket, tee: bool = True) -> subprocess.CompletedProcess:
    """
    Run a single tox command
    """
    os.environ["PY_COLORS"] = "1"
    arg_str = " ".join(command)
    click.echo(f"[tox-server] tox {arg_str}")
    args = ["tox"] + list(command)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)

    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, events=selectors.EVENT_READ, data=b"STDOUT")
    selector.register(proc.stderr, events=selectors.EVENT_READ, data=b"STDERR")

    streams = output_streams()

    while proc.poll() is None:
        for (key, events) in selector.select(timeout=0.01):
            channel = key.data
            data = cast(IO[bytes], key.fileobj).read(1024)
            output.send_multipart([channel, data])

            if tee:
                localfileobj = streams[channel]
                localfileobj.write(data)
                localfileobj.flush()

    return subprocess.CompletedProcess(args, returncode=proc.returncode)


@main.command()
@click.option("-t", "--tee/--no-tee", default=True, help="Write output locally as well as transmitting.")
@click.pass_context
def serve(ctx: click.Context, tee: bool = True) -> None:
    """
    Serve the tox server
    """
    cfg = ctx.find_object(dict)
    uri = f"tcp://0.0.0.0:{cfg['port']:d}"

    zctx = zmq.Context()
    socket = zctx.socket(zmq.REP)
    socket.bind(uri)

    stream_uri = f"tcp://0.0.0.0:{cfg['stream_port']:d}"
    output = zctx.socket(zmq.PUB)
    output.bind(stream_uri)

    with socket, output:
        click.echo(f"[tox-server] Serving on {uri}")
        click.echo(f"[tox-server] Output on {stream_uri}")
        click.echo("[tox-server] ^C to exit.")
        while True:

            socket.poll(timeout=1000)
            cmd, args = socket.recv_pyobj()
            if cmd == "QUIT":
                break
            elif cmd == "RUN":
                try:
                    if not all(isinstance(arg, str) for arg in args):
                        socket.send_pyobj(("ERR", TypeError(args)))
                        continue
                    r = run_tox_command(args, output, tee=tee)
                except Exception as e:
                    socket.send_pyobj(("ERR", repr(e)))
                else:
                    socket.send_pyobj(("RUN", r))
            elif cmd == "PING":
                socket.send_pyobj(("PONG", time.time()))
            else:
                socket.send_pyobj(("ERR", ValueError(f"Unknown command: {cmd}")))

        socket.send_pyobj("DONE")


@main.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("tox_args", nargs=-1, type=click.UNPROCESSED)
def run(ctx: click.Context, tox_args: Tuple[bytes]) -> None:
    """
    Run a tox command on the server.

    All arguments are forwarded to `tox` on the host machine.
    """
    timeout = 10
    cfg = ctx.find_object(dict)

    zctx = zmq.Context.instance()
    output = zctx.socket(zmq.SUB)
    output.connect(f"tcp://{cfg['host']}:{cfg['stream_port']:d}")
    output.subscribe(b"")

    poller = zmq.Poller()
    streams = output_streams()

    start = time.monotonic()

    with output, client(cfg["host"], cfg["port"]) as socket:
        poller.register(socket, flags=zmq.POLLIN)
        poller.register(output, flags=zmq.POLLIN)

        socket.send_pyobj(("RUN", tox_args))
        while True:
            events = dict(poller.poll(timeout=10))
            if output in events:
                start = time.monotonic()
                chan, data = output.recv_multipart()
                stream = streams[chan]
                stream.write(data)
                stream.flush()
            elif socket in events:
                break

            if time.monotonic() - start > timeout:
                click.echo("Command timed out!", err=True)
                raise click.Abort()

        reply, info = socket.recv_pyobj()
        if reply == "ERR":
            click.echo(repr(info), err=True)
            raise click.Abort()
        elif reply == "RUN":
            response: subprocess.CompletedProcess = info
    if response.returncode == 0:
        click.echo(f"[{click.style('DONE', fg='green')}] passed")
    else:
        click.echo(f"[{click.style('FAIL', fg='red')}] failed")
    sys.exit(response.returncode)


@main.command()
@click.option("-t", "--timeout", type=int, default=10, help="Timeout for waiting on a response (ms).")
@click.pass_context
def quit(ctx: click.Context, timeout: int) -> None:
    """
    Quit the server.
    """
    cfg = ctx.find_object(dict)

    with client(cfg["host"], cfg["port"]) as socket:
        socket.send_pyobj(("QUIT", None))
        if socket.poll(timeout=timeout):
            response, now = socket.recv_pyobj()
        else:
            click.echo("Socket timeout")
            raise click.Abort
        response = socket.recv_pyobj()

    click.echo(response)


@main.command()
@click.option("-t", "--timeout", type=int, default=10, help="Timeout for waiting on a response (ms).")
@click.pass_context
def ping(ctx: click.Context, timeout: int) -> None:
    """
    Ping the server, to check if it is alive.
    """
    cfg = ctx.find_object(dict)

    with client(cfg["host"], cfg["port"]) as socket:
        socket.send_pyobj(("PING", None))
        if socket.poll(timeout=timeout):
            response, now = socket.recv_pyobj()
        else:
            click.echo("Socket timeout")
            raise click.Abort
    click.echo(response)


if __name__ == "__main__":
    main()

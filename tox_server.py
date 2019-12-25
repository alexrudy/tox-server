#!/usr/bin/env python3
import os
import selectors
import subprocess
import sys
import time
import uuid
from typing import cast
from typing import Dict
from typing import IO
from typing import List
from typing import Tuple
from typing import Any

import click
import zmq


def recv(socket: zmq.Socket, timeout: int = 0, flags: int = 0) -> Tuple[str, Any]:
    data = socket.recv_json()
    return data["command"], data["args"]


def send(socket: zmq.Socket, command: str, args: Any, timeout: int = 0):
    socket.poll(flags=zmq.POLLOUT, timeout=timeout)
    socket.send_json({"command": command, "args": args}, flags=zmq.NOBLOCK)


@click.group()
@click.option(
    "-p",
    "--port",
    type=int,
    envvar="TOX_SERVER_PORT",
    help="Port to connect for tox-server",
    required=True,
)
@click.option(
    "-s",
    "--stream-port",
    type=int,
    envvar="TOX_SERVER_STREAM_PORT",
    help="Port to connect for tox-server stream output",
)
@click.option(
    "-h",
    "--host",
    type=str,
    envvar="TOX_SERVER_HOST",
    default="localhost",
    help="Host to connect for tox-server",
)
@click.option(
    "-b",
    "--bind-host",
    type=str,
    envvar="TOX_SERVER_BIND_HOST",
    default="127.0.0.1",
    help="Host to connect for tox-server",
)
@click.pass_context
def main(
    ctx: click.Context, host: str, port: int, stream_port: int, bind_host: str
) -> None:
    """Interact with a tox server."""
    cfg = ctx.ensure_object(dict)

    if stream_port is None:
        stream_port = port + 1

    cfg["host"] = host
    cfg["port"] = port
    cfg["stream_port"] = stream_port
    cfg["bind_host"] = bind_host


def client(host: str, port: int) -> zmq.Socket:
    zctx = zmq.Context.instance()
    socket = zctx.socket(zmq.REQ)
    socket.connect(f"tcp://{host:s}:{port:d}")
    socket.set(zmq.LINGER, 10)
    return socket


def output_streams(channel: bytes) -> Dict[bytes, IO[bytes]]:
    return {
        channel + b"STDERR": getattr(sys.stderr, "buffer", sys.stderr),
        channel + b"STDOUT": getattr(sys.stdout, "buffer", sys.stdout),
    }


def run_tox_command(
    command: List[str], output: zmq.Socket, channel: str, tee: bool = True
) -> subprocess.CompletedProcess:
    """
    Run a single tox command
    """
    os.environ.setdefault("PY_COLORS", "1")
    arg_str = " ".join(command)
    click.echo(f"[tox-server] tox {arg_str}")
    args = ["tox"] + list(command)
    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0
    )

    bchan = channel.encode("utf-8")

    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, events=selectors.EVENT_READ, data=bchan + b"STDOUT")
    selector.register(proc.stderr, events=selectors.EVENT_READ, data=bchan + b"STDERR")

    streams = output_streams(channel=bchan)

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
@click.option(
    "-t",
    "--tee/--no-tee",
    default=True,
    help="Write output locally as well as transmitting.",
)
@click.pass_context
def serve(ctx: click.Context, tee: bool = True) -> None:
    """
    Serve the tox server
    """
    cfg = ctx.find_object(dict)
    uri = f"tcp://{cfg['bind_host']}:{cfg['port']:d}"

    zctx = zmq.Context()
    socket = zctx.socket(zmq.REP)
    socket.bind(uri)

    stream_uri = f"tcp://{cfg['bind_host']}:{cfg['stream_port']:d}"
    output = zctx.socket(zmq.PUB)
    output.bind(stream_uri)

    with socket, output:
        click.echo(f"[tox-server] Serving on {uri}")
        click.echo(f"[tox-server] Output on {stream_uri}")
        click.echo("[tox-server] ^C to exit.")
        while True:
            if not socket.poll(timeout=1000):
                continue
            cmd, args = recv(socket, timeout=0)
            if cmd == "QUIT":
                break
            elif cmd == "RUN":
                try:
                    if not all(isinstance(arg, str) for arg in args):
                        send(socket, "ERR", repr(TypeError(args)))
                        continue
                    r = run_tox_command(
                        args["tox"], output, channel=args["channel"], tee=tee
                    )
                except Exception as e:
                    send(socket, "ERR", repr(e))
                else:
                    send(socket, "RUN", {"returncode": r.returncode, "args": r.args})

            elif cmd == "PING":

                send(socket, "PONG", {"time": time.time()})
            else:
                send(socket, "ERR", repr(ValueError(f"Unknown command: {cmd}")))

        send(socket, "DONE", "DONE")


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

    channel = str(uuid.uuid4())

    zctx = zmq.Context.instance()
    output = zctx.socket(zmq.SUB)
    output.connect(f"tcp://{cfg['host']}:{cfg['stream_port']:d}")
    output.subscribe(channel.encode("utf-8"))

    poller = zmq.Poller()
    streams = output_streams(channel.encode("utf-8"))

    start = time.monotonic()

    with output, client(cfg["host"], cfg["port"]) as socket:
        poller.register(socket, flags=zmq.POLLIN)
        poller.register(output, flags=zmq.POLLIN)

        send(socket, "RUN", {"tox": tox_args, "channel": channel}, timeout=0)
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

        reply, info = recv(socket, timeout=0)
        if reply == "ERR":
            click.echo(repr(info), err=True)
            raise click.Abort()
        elif reply == "RUN":
            response = subprocess.CompletedProcess(
                args=info["args"], returncode=info["returncode"]
            )
    if response.returncode == 0:
        click.echo(f"[{click.style('DONE', fg='green')}] passed")
    else:
        click.echo(f"[{click.style('FAIL', fg='red')}] failed")
    sys.exit(response.returncode)


@main.command()
@click.option(
    "-t",
    "--timeout",
    type=int,
    default=10,
    help="Timeout for waiting on a response (ms).",
)
@click.pass_context
def quit(ctx: click.Context, timeout: int) -> None:
    """
    Quit the server.
    """
    cfg = ctx.find_object(dict)

    with client(cfg["host"], cfg["port"]) as socket:
        send(socket, "QUIT", None, timeout=0)
        if socket.poll(timeout=timeout):
            response, info = recv(socket, timeout=0)
        else:
            click.echo("Socket timeout")
            raise click.Abort

    click.echo(info)


@main.command()
@click.option(
    "-t",
    "--timeout",
    type=int,
    default=10,
    help="Timeout for waiting on a response (ms).",
)
@click.pass_context
def ping(ctx: click.Context, timeout: int) -> None:
    """
    Ping the server, to check if it is alive.
    """
    cfg = ctx.find_object(dict)

    with client(cfg["host"], cfg["port"]) as socket:
        send(socket, "PING", None)
        if socket.poll(timeout=timeout):
            response, now = recv(socket)
        else:
            click.echo("Socket timeout")
            raise click.Abort
    click.echo(response)


if __name__ == "__main__":
    main()

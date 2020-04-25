#!/usr/bin/env python3
import asyncio
import os
import subprocess
import logging
import shlex
import sys
import time
import uuid
from typing import Optional
from typing import IO
from typing import List
from typing import Tuple
from typing import Any

import click
import zmq
import zmq.asyncio

log = logging.getLogger(__name__)


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
    logging.basicConfig(
        format=f"[{ctx.invoked_subcommand}] %(message)s", level=logging.INFO
    )


def client(host: str, port: int) -> zmq.Socket:
    zctx = zmq.Context.instance()
    socket = zctx.socket(zmq.REQ)
    uri = f"tcp://{host:s}:{port:d}"
    socket.connect(uri)
    socket.set(zmq.LINGER, 10)
    click.echo(f"Connected to {uri}")
    return socket


class LocalStreams:
    def __getitem__(self, key: bytes) -> IO[bytes]:
        if key.endswith(b"STDERR"):
            return getattr(sys.stderr, "buffer", sys.stderr)
        elif key.endswith(b"STDOUT"):
            return getattr(sys.stdout, "buffer", sys.stdout)
        else:
            raise KeyError(key)

    def __contains__(self, key: bytes) -> bool:
        return key.endswith(b"STDOUT") or key.endswith(b"STDERR")


async def send_command(socket: zmq.asyncio.Socket, command: str, args: Any) -> None:
    await socket.send_json({"command": command, "args": args})


async def tox_command(
    args: List[str], channel: str, output: zmq.asyncio.Socket, tee: bool = True
) -> subprocess.CompletedProcess:
    """Cause a tox command to be run asynchronously"""
    os.environ.setdefault("PY_COLORS", "1")
    arg_str = "tox " + " ".join(shlex.quote(a) for a in args)
    log.info(f"{arg_str}")

    proc = await asyncio.subprocess.create_subprocess_shell(
        arg_str, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    assert proc.stdout is not None, "Expected to get a STDOUT stream from asyncio"
    assert proc.stderr is not None, "Expected to get a STDERR stream from asyncio"

    bchan = channel.encode("utf-8")

    output_tasks = {
        asyncio.create_task(
            publish_output(proc.stdout, output, bchan + b"STDOUT", tee=tee)
        ),
        asyncio.create_task(
            publish_output(proc.stderr, output, bchan + b"STDERR", tee=tee)
        ),
    }

    try:
        returncode = await proc.wait()
    except asyncio.CancelledError:
        proc.terminate()
        raise

    finally:

        log.info(f"exit={returncode}")
        for task in output_tasks:
            task.cancel()

        await asyncio.wait(output_tasks)

        for task in output_tasks:
            try:
                task.result()
            except asyncio.CancelledError:
                log.info("Cancelled stream")
            except Exception:
                log.exception("Output Exception")

    log.info(f"done")

    return subprocess.CompletedProcess(args, returncode=returncode)


async def publish_output(
    stream: asyncio.StreamReader,
    socket: zmq.asyncio.Socket,
    channel: bytes,
    tee: bool = False,
) -> None:
    """Publish stream data to a ZMQ socket"""
    streams = LocalStreams()

    while True:
        data = await stream.read(n=1024)
        if data:
            await socket.send_multipart([channel, data])
        else:
            # No data was recieved, but we should yield back
            # to the event loop so we don't get stuck here.
            # Ideally, we'd not let .read() return with 0 bytes
            # but that doesn't seem to be possible with asyncio?
            await asyncio.sleep(0.1)
        if tee:
            localstream = streams[channel]
            localstream.write(data)
            localstream.flush()


class Server:
    def __init__(
        self,
        control_uri: str,
        output_uri: str,
        zctx: Optional[zmq.asyncio.Context] = None,
    ) -> None:
        self.control_uri = control_uri
        self.zctx = zctx or zmq.asyncio.Context.instance()

        self.socket = self.zctx.socket(zmq.REP)
        self.socket.bind(self.control_uri)

        self.output = self.zctx.socket(zmq.PUB)
        self.output.bind(output_uri)

        self.running = False

    async def run_forever(self) -> None:
        self.running = True
        while self.running:
            await self.handle_command()
        await send_command(self.socket, "QUIT", "DONE")
        self.socket.close()
        self.output.close()

    async def handle_command(self) -> None:
        data = await self.socket.recv_json()
        cmd, args = data["command"], data["args"]
        log.info(f"Command: {cmd} | {args!r}")

        try:
            await getattr(self, f"handle_{cmd.lower()}")(args)
        except AttributeError:
            await send_command(
                self.socket, "ERR", {"command": cmd, "message": "Unknown command"}
            )

    async def handle_run(self, args: Any) -> None:
        result = await tox_command(args["tox"], args["channel"], self.output)
        await send_command(
            self.socket, "RUN", {"returncode": result.returncode, "args": result.args},
        )

    async def handle_quit(self, args: Any) -> None:
        self.running = False

    async def handle_ping(self, args: Any) -> None:
        await send_command(self.socket, "PONG", {"time": time.time()})


def unparse_arguments(args: Tuple[str, ...]) -> Tuple[str, ...]:
    """
    Un-does some parsing that click does to long argument sets.

    Click will swallow an empty option ('--'), but we'd like to pass that empty
    option on to tox on the remote server. This function re-adds the empty option
    if it was present on the command line.
    """
    try:
        idx = sys.argv.index("--") - (len(sys.argv) - len(args) - 1)
    except ValueError:
        pass
    else:
        ta = list(args)
        ta.insert(idx, "--")
        args = tuple(ta)
    return args


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
    control_uri = f"tcp://{cfg['bind_host']}:{cfg['port']:d}"
    output_uri = f"tcp://{cfg['bind_host']}:{cfg['stream_port']:d}"

    server = Server(control_uri, output_uri)

    asyncio.run(server.run_forever())


@main.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("tox_args", nargs=-1, type=click.UNPROCESSED)
def run(ctx: click.Context, tox_args: Tuple[str, ...]) -> None:
    """
    Run a tox command on the server.

    All arguments are forwarded to `tox` on the host machine.
    """
    tox_args = unparse_arguments(tox_args)
    timeout = 10
    cfg = ctx.find_object(dict)

    channel = str(uuid.uuid4())

    zctx = zmq.Context.instance()
    output = zctx.socket(zmq.SUB)
    output.connect(f"tcp://{cfg['host']}:{cfg['stream_port']:d}")
    output.subscribe(channel.encode("utf-8"))

    poller = zmq.Poller()
    streams = LocalStreams()

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
            response: subprocess.CompletedProcess = subprocess.CompletedProcess(
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

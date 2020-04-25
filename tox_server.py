#!/usr/bin/env python3
import asyncio
import os
import subprocess
import logging
import shlex
import sys
import time
import uuid
import json
import enum
import dataclasses as dc
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
    # socket.poll(flags=zmq.POLLOUT, timeout=timeout)
    socket.send_json({"command": command, "args": args}, flags=zmq.NOBLOCK)


class Command(enum.Enum):
    ERR = enum.auto()
    QUIT = enum.auto()
    PING = enum.auto()
    PONG = enum.auto()
    RUN = enum.auto()


@dc.dataclass
class Message:

    command: Command
    args: Any

    identifiers: Optional[List[bytes]] = None

    @classmethod
    def parse(cls, message: List[bytes]) -> "Message":
        if not message:
            raise ProtocolFailure("No message parts recieved")

        *identifiers, mpart = message

        if identifiers and not identifiers[-1] == b"":
            raise ProtocolFailure(f"Invalid multipart identifiers: {identifiers!r}")

        try:
            mdata = json.loads(mpart)
        except (TypeError, ValueError) as e:
            raise ProtocolError.from_message(
                message=f"JSON decode error: {e}", identifiers=identifiers
            )

        try:
            command, args = mdata["command"], mdata["args"]
        except KeyError as e:
            raise ProtocolError.from_message(
                message=f"JSON missing key: {e}", identifiers=identifiers
            )

        try:
            command = Command[command]
        except KeyError:
            raise ProtocolError.from_message(
                message=f"Unknown command: {command}", identifiers=identifiers
            )

        return cls(command=command, args=args, identifiers=identifiers)

    def assemble(self) -> List[bytes]:
        message = json.dumps({"command": self.command.name, "args": self.args}).encode(
            "utf-8"
        )
        if self.identifiers:
            return self.identifiers + [message]
        return [message]

    def respond(self, command: Command, args: Any) -> "Message":
        return self.__class__(command=command, args=args, identifiers=self.identifiers)


@dc.dataclass
class ProtocolError(Exception):
    message: Message

    def __str__(self) -> str:
        return self.message.args["message"]

    @classmethod
    def from_message(
        cls, message: str, identifiers: Optional[List[bytes]] = None
    ) -> "ProtocolError":
        return cls(
            message=Message(Command.ERR, {"message": message}, identifiers=identifiers)
        )


@dc.dataclass
class ProtocolFailure(Exception):
    """A protocol error we can't recover from, the message was so malformed, no response is possible"""

    message: str


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

    control_uri = cfg["control_uri"] = f"tcp://{host}:{port:d}"
    logging.basicConfig(
        format=f"[{ctx.invoked_subcommand}] %(message)s", level=logging.INFO
    )


def client(uri: str) -> zmq.Socket:
    zctx = zmq.Context.instance()
    socket = zctx.socket(zmq.REQ)
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
        asyncio.ensure_future(
            publish_output(proc.stdout, output, bchan + b"STDOUT", tee=tee)
        ),
        asyncio.ensure_future(
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
        self.output_uri = output_uri
        self.zctx = zctx or zmq.asyncio.Context()

        self.running = False

    async def run_forever(self) -> None:
        self.running = True

        self.output = self.zctx.socket(zmq.PUB)
        self.output.bind(self.output_uri)

        self.socket = self.zctx.socket(zmq.REP)
        self.socket.bind(self.control_uri)

        log.info(f"Running server at {self.control_uri}")
        log.info(f"^C to exit")
        try:
            while self.running:
                await self.handle_command()
        except asyncio.CancelledError:
            log.info("Server cancelled")
        except BaseException:
            log.exception("Server loop error")
            raise
        finally:
            self.socket.close()
            self.output.close()
        log.debug(f"Server is done.")

    async def send(self, message: Message) -> None:
        log.debug(f".send {message!r}")
        await self.socket.send_multipart(message.assemble())

    async def handle_command(self) -> None:
        data = await self.socket.recv_multipart()
        log.debug(f"Data: {data!r}")

        try:
            msg = Message.parse(data)
        except ProtocolError as e:
            log.critical("Protocol error")
            await self.send(e.message)
            return
        except ProtocolFailure:
            log.critical("Protocol failure")
            raise

        log.debug(f"Message: {msg!r}")

        try:
            await getattr(self, f"handle_{msg.command.name.lower()}")(msg)
        except Exception as e:
            log.exception("Error in handler")
            err = msg.respond(command=Command.ERR, args={"message": str(e)})
            await self.send(err)

    async def handle_run(self, msg: Message) -> None:
        result = await tox_command(msg.args["tox"], msg.args["channel"], self.output)

        await self.send(
            msg.respond(
                Command.RUN, {"returncode": result.returncode, "args": result.args}
            )
        )

    async def handle_quit(self, msg: Message) -> None:
        self.running = False

        await self.send(msg.respond(Command.QUIT, "DONE"))

    async def handle_ping(self, msg: Message) -> None:
        await self.send(msg.respond(Command.PONG, {"time": time.time()}))


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

    try:
        server = Server(control_uri, output_uri)

        asyncio.run(server.run_forever())
    except BaseException:
        log.exception("Exception")
        raise


async def show_output(socket: zmq.asyncio.Socket):
    streams = LocalStreams()

    while True:
        chan, data = await socket.recv_multipart()
        stream = streams[chan]
        stream.write(data)
        stream.flush()


async def run_command(
    control_uri,
    output_uri,
    tox_args: Tuple[str, ...],
    channel: str,
    zctx: Optional[zmq.asyncio.Context] = None,
):
    zctx = zctx or zmq.asyncio.Context.instance()

    client = zctx.socket(zmq.REQ)
    client.connect(control_uri)

    output = zctx.socket(zmq.SUB)
    output.connect(output_uri)
    output.subscribe(channel.encode("utf-8"))

    message = Message(command=Command.RUN, args={"tox": tox_args, "channel": channel})

    with client, output:

        await client.send_multipart(message.assemble())
        output_task = asyncio.ensure_future(show_output(output))

        data = await client.recv_multipart()
        output_task.cancel()

        message = Message.parse(data)

    return message


@main.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@click.argument("tox_args", nargs=-1, type=click.UNPROCESSED)
def run(ctx: click.Context, tox_args: Tuple[str, ...]) -> None:
    """
    Run a tox command on the server.

    All arguments are forwarded to `tox` on the host machine.
    """
    tox_args = unparse_arguments(tox_args)
    cfg = ctx.find_object(dict)

    channel = str(uuid.uuid4())

    output_uri = f"tcp://{cfg['host']}:{cfg['stream_port']:d}"

    message = asyncio.run(
        run_command(cfg["control_uri"], output_uri, tox_args, channel)
    )

    if message.command == Command.ERR:
        click.echo(repr(message.args), err=True)
        raise click.Abort()
    elif message.command == Command.RUN:
        response: subprocess.CompletedProcess = subprocess.CompletedProcess(
            args=message.args["args"], returncode=message.args["returncode"]
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

    message = Message(command=Command.QUIT, args=None)
    try:
        response = asyncio.run(
            single_command(cfg["control_uri"], message, timeout=timeout)
        )
    except BaseException:
        log.exception("Exception")
        raise
    click.echo(response)


async def single_command(
    uri: str,
    message: Message,
    timeout: Optional[int] = None,
    zctx: Optional[zmq.asyncio.Context] = None,
) -> Any:
    zctx = zctx or zmq.asyncio.Context()

    socket = zctx.socket(zmq.REQ)
    socket.connect(uri)
    log.info(f"Connection to {uri}")

    log.info(f"Sending message {message!r}")

    task = asyncio.ensure_future(reqrep(socket, message.assemble()))
    await asyncio.wait_for(task, timeout=timeout)
    data = task.result()

    message = Message.parse(data)
    return message


async def reqrep(socket: zmq.asyncio.Socket, data: List[bytes]) -> List[bytes]:
    await socket.send_multipart(data)
    log.info(f"Waiting for response from {data!r}")
    return await socket.recv_multipart()


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

    message = Message(command=Command.PING, args=None)
    try:
        response = asyncio.run(
            single_command(cfg["control_uri"], message, timeout=timeout)
        )
    except BaseException:
        log.exception("Exception")
        raise
    click.echo(response)


async def echoer(control_uri: str):
    zctx = zmq.asyncio.Context()
    socket = zctx.socket(zmq.REP)
    socket.bind(control_uri)

    while True:
        message = await socket.recv_multipart()
        log.info(f"Message: {message!r}")
        await socket.send_multipart(message)


@main.command()
@click.pass_context
def echo(ctx: click.Context) -> None:
    """
    Ping the server, to check if it is alive.
    """
    cfg = ctx.find_object(dict)
    control_uri = f"tcp://{cfg['bind_host']}:{cfg['port']:d}"

    try:
        asyncio.run(echoer(control_uri))
    except BaseException:
        log.exception("Exception")
        raise


if __name__ == "__main__":
    main()

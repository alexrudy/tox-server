#!/usr/bin/env python3
import asyncio
import base64
import contextlib
import dataclasses as dc
import enum
import functools
import json
import logging
import os
import shlex
import signal
import subprocess
import sys
import time
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import IO
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import click
import zmq.asyncio

log = logging.getLogger(__name__)


class Stream(enum.Enum):
    """Stream switch for output"""

    STDOUT = enum.auto()
    STDERR = enum.auto()

    def _get_stream(self) -> IO[bytes]:
        """Return the stream object"""
        if self == Stream.STDERR:
            return getattr(sys.stderr, "buffer", sys.stderr)
        elif self == Stream.STDOUT:
            return getattr(sys.stdout, "buffer", sys.stdout)
        else:  # pragma: nocover
            raise ValueError(self)

    def fwrite(self, data: bytes) -> None:
        """Write and flush this stream"""
        s = self._get_stream()
        s.write(data)
        s.flush()


class Command(enum.Enum):
    """Intended effect of a command sent via ZMQ"""

    ERR = enum.auto()
    #: Indicates an error in command processing

    QUIT = enum.auto()
    #: Tells the server to quit

    PING = enum.auto()
    #: Asks the server for a heartbeat

    RUN = enum.auto()
    #: Run a tox command

    OUTPUT = enum.auto()
    #: Indicates tox output

    CANCEL = enum.auto()
    #: Cancel a running tox command


@dc.dataclass(frozen=True)
class Message:
    """Protocol message format"""

    command: Command
    #: Message command

    args: Any
    #: JSON-serializable arguments to this message

    identifiers: Optional[Tuple[bytes, ...]] = None
    #: ZMQ-specific socket identifiers

    @classmethod
    def parse(cls, message: List[bytes]) -> "Message":
        """Parse a list of ZMQ message frames

        Ensurses that the frames correspond to this protocol.

        Raises
        ------
        ProtocolFailure: When the message frames don't contain
            enough data to provide a safe response.
        ProtocolError: An error with the message frames, with a
            reply message indicating what went wrong.

        """
        if not message:
            raise ProtocolFailure("No message parts recieved")

        *identifiers, mpart = message

        if mpart == b"" and ((not identifiers) or identifiers[-1] != b""):
            raise ProtocolError.from_message(
                message=f"Invalid message missing content", identifiers=tuple(identifiers) + (b"",)
            )

        if identifiers and not identifiers[-1] == b"":
            raise ProtocolFailure(f"Invalid multipart identifiers missing delimiter: {identifiers!r}")

        try:
            mdata = json.loads(mpart)
        except (TypeError, ValueError) as e:
            raise ProtocolError.from_message(message=f"JSON decode error: {e}", identifiers=tuple(identifiers))

        try:
            command, args = mdata["command"], mdata["args"]
        except KeyError as e:
            raise ProtocolError.from_message(message=f"JSON missing key: {e}", identifiers=tuple(identifiers))

        try:
            command = Command[command]
        except KeyError:
            raise ProtocolError.from_message(message=f"Unknown command: {command}", identifiers=tuple(identifiers))

        return cls(command=command, args=args, identifiers=tuple(identifiers))

    def assemble(self) -> List[bytes]:
        """Assemble this message for sending"""
        message = json.dumps({"command": self.command.name, "args": self.args}).encode("utf-8")
        if self.identifiers:
            return list(self.identifiers) + [message]
        return [message]

    def respond(self, command: Command, args: Any) -> "Message":
        """Create a response message object, which retains message identifiers"""
        return dc.replace(self, command=command, args=args)

    def cancel(self) -> "Message":
        return dc.replace(self, command=Command.CANCEL, args={"command": self.command.name})

    @property
    def identifier(self) -> Tuple[bytes, ...]:
        """An immutable set of message identifiers"""
        if not self.identifiers:
            return (b"",)
        return tuple(self.identifiers)

    @classmethod
    async def recv(cls, socket: zmq.asyncio.Socket, flags: int = 0) -> "Message":
        """Recieve a message on the provided socket"""
        data = await socket.recv_multipart(flags=flags)
        return cls.parse(data)

    async def send(self, socket: zmq.asyncio.Socket, flags: int = 0) -> None:
        """Send a message on the provided socket"""
        await socket.send_multipart(self.assemble(), flags=flags)

    def for_dealer(self) -> "Message":
        return dc.replace(self, identifiers=tuple([b""] + list(self.identifiers or [])))


@dc.dataclass
class ProtocolError(Exception):
    """Error raised for a recoverable protocol problem.

    The :attr:`message` provides an error message to send back to the client.
    """

    message: Message

    def __str__(self) -> str:
        return self.message.args["message"]

    @classmethod
    def from_message(cls, message: str, identifiers: Optional[Tuple[bytes, ...]] = None) -> "ProtocolError":
        """Build a protocol error from a string message"""
        return cls(message=Message(Command.ERR, {"message": message}, identifiers=identifiers))


@dc.dataclass
class ProtocolFailure(Exception):
    """A protocol error we can't recover from, the message was so malformed, no response is possible"""

    message: str


async def tox_command(
    args: List[str], output: zmq.asyncio.Socket, message: Message, tee: bool = True
) -> subprocess.CompletedProcess:
    """Cause a tox command to be run asynchronously.

    Also arranges to send OUTPUT messages over the provided socket.

    Parameters
    ----------
    args: list
        Arguments to tox
    output: zmq.asyncio.Socket
        Asynchronous ZMQ socket for sending output messages from the subprocess.
    message: Message
        Message used to set up replies on the output socket.
    tee: bool
        If set (default), also print output to local streams.

    Returns
    -------
    process: subprocess.CompletedProcess
        Named tuple summarizing the process.

    """
    os.environ.setdefault("PY_COLORS", "1")
    arg_str = "tox " + " ".join(shlex.quote(a) for a in args)
    log.info(f"command: {arg_str}")

    proc = await asyncio.subprocess.create_subprocess_shell(
        arg_str, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    log.debug(f"Launched proc {proc!r}")

    assert proc.stdout is not None, "Expected to get a STDOUT stream from asyncio"
    assert proc.stderr is not None, "Expected to get a STDERR stream from asyncio"

    log.debug(f"Launching output tasks")

    output_tasks = {
        asyncio.create_task(publish_output(proc.stdout, output, message, Stream.STDOUT, tee=tee)),
        asyncio.create_task(publish_output(proc.stderr, output, message, Stream.STDERR, tee=tee)),
    }

    log.debug(f"Waiting for subprocess")

    try:
        returncode = await proc.wait()
    except asyncio.CancelledError:
        log.debug(f"Cancelling subprocess")
        proc.terminate()
        returncode = await proc.wait()
    except Exception:  # pragma: nocover
        log.exception("Exception in proc.wait")
        raise
    finally:
        log.debug(f"Cleaning up")
        for task in output_tasks:
            task.cancel()

        await asyncio.wait(output_tasks)

    log.info(f"command done")

    return subprocess.CompletedProcess(args, returncode=returncode)


async def publish_output(
    reader: asyncio.StreamReader, socket: zmq.asyncio.Socket, message: Message, stream: Stream, tee: bool = False
) -> None:
    """Publish stream data to a ZMQ socket.

    Parameters
    ----------
    reader: asyncio.StreamReader
        Source reader for providing data
    socket: zmq.asyncio.Socket
        Socket for output messages
    message: Message
        Message template (provides identifiers)
    stream: Stream
        Enum identifying the stream in question (STDERR/STDOUT)
    tee: bool
        Whether to also send data to the local versions of the
        requested streams.

    """

    while True:
        data = await reader.read(n=1024)
        if data:
            message = message.respond(
                Command.OUTPUT, args={"data": base64.b85encode(data).decode("ascii"), "stream": stream.name}
            )
            await message.send(socket)
        else:
            # No data was recieved, but we should yield back
            # to the event loop so we don't get stuck here.
            # Ideally, we'd not let .read() return with 0 bytes
            # but that doesn't seem to be possible with asyncio?
            await asyncio.sleep(0.1)
        if tee:
            stream.fwrite(data)


class Server:
    """
    Manages the asynchronous response to commands for the tox server.

    To launch the server, call :meth:`serve_forever`.

    Parameters
    ----------
    uri: str
        ZMQ-style bind URI for the server. Will be bound to a ROUTER socket.
    zctx: zmq.asyncio.Context, optional
        ZMQ asynchronous context for creating sockets.

    """

    def __init__(self, uri: str, tee: bool = False, zctx: Optional[zmq.asyncio.Context] = None) -> None:
        self.uri = uri
        self.tee = tee
        self.zctx = zctx or zmq.asyncio.Context()

        self.tasks: Dict[Tuple[Command, Tuple[bytes, ...]], asyncio.Future] = {}
        self._timeout = 0.1

    async def serve_forever(self) -> None:
        """Start the server, and ensure it runs until this coroutine is cancelled

        To cancel the server, start this method as a task, then cancel the task::

            async def main():
                server = Server("inproc://control")
                task = asyncio.create_task(server.serve_forever)
                task.cancel()
                await task

            asyncio.run(main)

        """

        # Event used to indicate that the server should finish gracefully.
        self.shutdown = asyncio.Event()

        # The lock is used to ensure that only a single tox subprocess
        # can run at any given time.
        self.lock = asyncio.Lock()

        # Tracks pending coroutines
        self.tasks = {}

        self.socket = self.zctx.socket(zmq.ROUTER)
        self.socket.bind(self.uri)

        log.info(f"Running server at {self.uri}")
        log.info(f"^C to exit")
        try:
            task = asyncio.create_task(self.process())
            await self.shutdown.wait()
            await self.drain()
        except asyncio.CancelledError:
            log.info("Server cancelled")
        except BaseException:  # pragma: nocover
            log.exception("Server loop error")
            raise
        finally:
            task.cancel()
            self.socket.close()
        log.debug(f"Server is done.")

    async def send(self, message: Message) -> None:
        """Send a message over the server's ZMQ socket

        Parameters
        ----------
        message: Message
            Message to be sent.

        """
        log.debug(f"Send: {message!r}")
        await message.send(self.socket)

    async def drain(self) -> None:
        """Ensure any remianing tasks are awaited"""
        if self.tasks:
            await asyncio.wait(self.tasks.values())

    async def process(self) -> None:
        """Process message events, dispatching handling to other futures.

        Each message recieved starts a new task using :meth:`handle`. Tasks
        are tracked using message identifiers, which are the identification
        frames provided by ZMQ.

        """
        while not self.shutdown.is_set():
            with contextlib.suppress(ProtocolError):
                msg = await self.recv()
                if (msg.command, msg.identifier) in self.tasks:
                    await self.send(
                        msg.respond(
                            Command.ERR, args={"message": "A task has already started.", "command": msg.command.name}
                        )
                    )
                else:
                    self.tasks[(msg.command, msg.identifier)] = asyncio.create_task(self.handle(msg))

    async def recv(self) -> Message:
        """Recieve a message which conforms to the tox-server protocol

        Messages which don't conform, raise errors. If socket identity information
        is provided, error messages can be returned to the sender
        (indicated by :exc:`ProtocolError`). Some messages don't contain enough information
        to be certain of the sender, and so raise :exc:`ProtocolFailure` instead.

        Returns
        -------
        message: Message
            Incoming message object for transmission.

        """
        try:
            msg = await Message.recv(self.socket)
        except ProtocolError as e:
            log.exception("Protocol error")
            await self.send(e.message)
            raise
        except ProtocolFailure:  # pragma: nocover
            log.exception("Protocol failure")

        log.debug(f"Recieve: {msg!r}")
        return msg

    async def handle(self, msg: Message) -> None:
        """Handle a single tox-server protocol message, and respond.

        This coroutine is designed to be run as its own task, and will
        respond on the socket when appropriate. It delegates command
        work to methods with names of the form ``handle_<command>`` where
        ``<command>`` is the name of the command enum in lower case.
        """
        try:
            await getattr(self, f"handle_{msg.command.name.lower()}")(msg)
        except asyncio.CancelledError:  # pragma: nocover
            raise
        except Exception as e:
            log.exception("Error in handler")
            err = msg.respond(command=Command.ERR, args={"message": str(e)})
            await self.send(err)
        finally:
            self.tasks.pop((msg.command, msg.identifier), None)
            log.debug(f"Finished handling {msg!r}")

    async def handle_cancel(self, msg: Message) -> None:
        """Handles a cancel message

        If the client has sent a message and that message is processing,
        it will be cancelled. Any task can be cancelled, but this is really
        only useful for cancelling RUN tasks.
        """
        target = Command[msg.args["command"]]
        task = self.tasks.pop((target, msg.identifier), None)
        if task:
            log.debug(f"Cancelling {task!r}")
            task.cancel()
        else:
            log.debug(f"Task to cancel not found: {msg!r}")
            await self.send(msg.respond(Command.ERR, {"message": "Could not find task"}))

    async def handle_run(self, msg: Message) -> None:
        """Handles a run message to start a tox subprocess.

        Runs are controlled by :func:`tox_command`, and locked by the
        server to ensure that only one tox process runs at a time.
        """
        async with self.lock:
            result = await tox_command(msg.args["tox"], self.socket, msg, tee=self.tee)
        await self.send(msg.respond(Command.RUN, {"returncode": result.returncode, "args": result.args}))

    async def handle_quit(self, msg: Message) -> None:
        """Handles a quit message to gracefully shut down.

        Quit starts a graceful shutdown process for the server.
        """
        self.shutdown.set()
        log.debug("Requesting shutdown")
        await self.send(msg.respond(Command.QUIT, "DONE"))

    async def handle_ping(self, msg: Message) -> None:
        """Handles a ping message to check the server liveliness"""
        await self.send(msg.respond(Command.PING, {"time": time.time(), "tasks": len(self.tasks)}))


def unparse_arguments(args: Tuple[str, ...]) -> Tuple[str, ...]:
    """
    Un-does some parsing that click does to long argument sets.

    Click will swallow an empty option ('--'), but we'd like to pass that empty
    option on to tox on the remote server. This function re-adds the empty option
    if it was present on the command line.
    """
    if not all(arg in sys.argv for arg in args):
        return args

    try:
        idx = sys.argv.index("--") - (len(sys.argv) - len(args) - 1)
    except ValueError:
        pass
    else:
        ta = list(args)
        ta.insert(idx, "--")
        args = tuple(ta)
    return args


class LogLevelParamType(click.ParamType):
    name = "log_level"

    def convert(self, value: str, param: Any, ctx: Any) -> int:
        try:
            return int(value)
        except TypeError:
            self.fail(
                "expected string for int() conversion, got " f"{value!r} of type {type(value).__name__}", param, ctx
            )
        except ValueError:
            try:
                return getattr(logging, value.upper())
            except AttributeError:
                pass

            self.fail(f"{value!r} is not a valid integer", param, ctx)


@click.group()
@click.option("-p", "--port", type=int, envvar="TOX_SERVER_PORT", help="Port to connect for tox-server.", required=True)
@click.option(
    "-h", "--host", type=str, envvar="TOX_SERVER_HOST", default="localhost", help="Host to connect for tox-server."
)
@click.option(
    "-b",
    "--bind-host",
    type=str,
    envvar="TOX_SERVER_BIND_HOST",
    default="127.0.0.1",
    help="Host to bind for tox-server serve.",
)
@click.option(
    "-t",
    "--timeout",
    type=float,
    default=None,
    help="Timeout for waiting on a response (s).",
    envvar="TOX_SERVER_TIMEOUT",
)
@click.option("-l", "--log-level", type=LogLevelParamType(), help="Set logging level", default=logging.WARNING)
@click.pass_context
def main(ctx: click.Context, host: str, port: int, bind_host: str, timeout: Optional[float], log_level: int) -> None:
    """Interact with a tox server."""
    cfg = ctx.ensure_object(dict)

    cfg["uri"] = f"tcp://{host}:{port:d}"
    cfg["bind"] = f"tcp://{bind_host}:{port:d}"
    cfg["timeout"] = timeout
    logging.basicConfig(format=f"[{ctx.invoked_subcommand}] %(message)s", level=log_level)


@main.command()
@click.option("-t", "--tee/--no-tee", default=True, help="Write output locally as well as transmitting.")
@click.pass_context
def serve(ctx: click.Context, tee: bool = True) -> None:
    """
    Run the tox server.

    The server runs indefinitely until it recieves a quit command or a sigint
    """
    cfg = ctx.find_object(dict)

    try:
        server = Server(cfg["bind"], tee=tee)
        asyncio.run(server.serve_forever())
    except BaseException:
        log.exception("Exception in server")
        raise


async def client(
    uri: str, message: Message, timeout: Optional[float] = None, zctx: Optional[zmq.asyncio.Context] = None
) -> Message:
    """Manage client connection to tox-server

    This coroutine sends a command. It also sets up an interrput singal handler, so
    that ^C is propogated to the server as a CANCEL command.

    Parameters
    ----------
    uri: str
        ZMQ-style connect URI to find the command server.
    message: Message
        Tox-server protocol message to send to the server
    timeout: float, optional
        Length of time to wait for any communication. Each message recieved will
        reset this timeout.
    zctx: zmq.asyncio.Context, optional
        ZMQ context to use when building the socket connection.
    """
    zctx = zctx or zmq.asyncio.Context.instance()

    client = zctx.socket(zmq.DEALER)
    client.connect(uri)
    client.setsockopt(zmq.LINGER, 0)

    sigint_callback = functools.partial(send_interrupt, socket=client, message=message)

    with interrupt_handler(signal.SIGINT, sigint_callback, oneshot=True), client:

        await message.for_dealer().send(client)

        while True:
            response = await asyncio.wait_for(Message.recv(client), timeout=timeout)
            if response.command == Command.OUTPUT:
                Stream[response.args["stream"]].fwrite(base64.b85decode(response.args["data"]))
            else:
                # Note: this assumes that OUTPUT is the only command which shouldn't end
                # the await loop above, which might not be true...
                break

    return response


@contextlib.contextmanager
def interrupt_handler(sig: int, task_factory: Callable[[], Awaitable[Any]], oneshot: bool = False) -> Iterator[None]:
    """Adds an async interrupt handler to the event loop for the context block.

    The interrupt handler should be a callable task factory, which will create
    a coroutine which should run when the signal is recieved. `task_factory` should
    not accept any arguments.

    Parameters
    ----------
    signal: int
        Signal number (see :mod:`signal` for interrupt handler)
    task_factory: callable
        Factory function to create awaitable tasks which will be run when
        the signal is recieved.

    """
    loop = asyncio.get_running_loop()

    sig_oneshot: Optional[int]
    if oneshot:
        sig_oneshot = sig
    else:
        sig_oneshot = None

    loop.add_signal_handler(
        sig, functools.partial(signal_interrupt_handler, loop=loop, task_factory=task_factory, sig=sig_oneshot)
    )

    yield

    loop.remove_signal_handler(sig)


async def send_interrupt(socket: zmq.asyncio.Socket, message: Message) -> None:
    """Helper function to send a cancel message using the provided socket"""
    interrupt = message.cancel().for_dealer()
    log.debug(f"Sending interrupt: {interrupt!r}")
    click.echo("", err=True)
    click.echo(f"Cancelling {command_name()} with the server. ^C again to exit.", err=True)
    await interrupt.send(socket)


def signal_interrupt_handler(
    loop: asyncio.AbstractEventLoop, task_factory: Callable, sig: Optional[int] = None
) -> None:
    """
    Callback used for :meth:`asyncio.AbstractEventLoop.add_signal_handler`
    to schedule a coroutine when a signal is recieved.
    """
    if sig is not None:
        loop.remove_signal_handler(sig)

    loop.create_task(task_factory())


def command_name() -> str:
    return click.get_current_context().command.name


def run_client(uri: str, message: Message, timeout: Optional[float] = None) -> Message:
    """Wrapper function to run a client from a CLI"""
    try:
        response = asyncio.run(client(uri, message, timeout=timeout), debug=True)
    except asyncio.TimeoutError:
        click.echo(f"Command {command_name()} timed out!", err=True)
        raise SystemExit(2)
    except KeyboardInterrupt:
        click.echo("", err=True)
        click.echo(f"Command {command_name()} interrupted!", err=True)
        raise SystemExit(3)
    except BaseException:  # pragma: nocover
        log.exception("Unhandled exception in asyncio loop")
        raise
    else:
        if response.command == Command.ERR:
            log.error(f"Error in command: {response!r}")
            click.echo(repr(message.args), err=True)
            click.echo(f"Command {command_name()} error!", err=True)
            raise SystemExit(1)
        return response


@main.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.pass_context
def run(ctx: click.Context) -> None:
    """
    Run a tox command on the server.

    All arguments are forwarded to `tox` on the host machine.
    """
    tox_args = unparse_arguments(tuple(ctx.args))
    cfg = ctx.find_object(dict)

    message = Message(command=Command.RUN, args={"tox": tox_args})
    response = run_client(cfg["uri"], message, timeout=cfg["timeout"])

    proc: subprocess.CompletedProcess = subprocess.CompletedProcess(
        args=response.args["args"], returncode=response.args["returncode"]
    )
    if proc.returncode == 0:
        click.echo(f"[{click.style('DONE', fg='green')}] passed")
    else:
        click.echo(f"[{click.style('FAIL', fg='red')}] failed")
    sys.exit(proc.returncode)


@main.command()
@click.pass_context
def quit(ctx: click.Context) -> None:
    """
    Quit the server.
    """
    cfg = ctx.find_object(dict)

    message = Message(command=Command.QUIT, args=None)
    response = run_client(cfg["uri"], message, timeout=cfg["timeout"])
    click.echo(response.args)


@main.command()
@click.pass_context
def ping(ctx: click.Context) -> None:
    """
    Ping the server, to check if it is alive.
    """
    cfg = ctx.find_object(dict)
    message = Message(command=Command.PING, args=None)
    response = run_client(cfg["uri"], message, timeout=cfg["timeout"])
    click.echo(response.args)


if __name__ == "__main__":
    main()

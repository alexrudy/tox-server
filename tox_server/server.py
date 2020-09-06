import asyncio
import contextlib
import dataclasses as dc
import enum
import logging
import signal
import time
from typing import Dict
from typing import Generator
from typing import Optional
from typing import Set
from typing import Tuple

import click
import zmq.asyncio

from .interrupt import interrupt_handler
from .process import tox_command
from .protocol import Command
from .protocol import Message
from .protocol import ProtocolError
from .protocol import ProtocolFailure

log = logging.getLogger(__name__)

__all__ = ["Server", "serve"]


class TaskState(enum.Enum):
    INIT = 0
    QUEUED = 1
    STARTED = 2


TaskID = Tuple[Command, Tuple[bytes, ...]]


@dc.dataclass(init=False)
class Task:

    message: Message
    action: asyncio.Future
    heartbeat: asyncio.Future
    state: TaskState = TaskState.INIT

    def __init__(self, message: Message, server: "Server") -> None:
        super().__init__()
        self.message = message
        self.heartbeat = asyncio.create_task(server.run_heartbeat(self))
        self.action = asyncio.create_task(server.handle(self))
        self.state = TaskState.QUEUED

    def __hash__(self) -> int:
        return hash((self.__class__.__qualname__, self.action, self.heartbeat))

    @property
    def id(self) -> TaskID:
        return (self.message.command, self.message.identifier)

    def start(self) -> None:
        self.state = TaskState.STARTED

    async def beat(self, socket: zmq.asyncio.Socket) -> float:
        state = self.state.name
        msg = self.message.respond(Command.HEARTBEAT, args={"now": time.time(), "state": state})
        await msg.send(socket)

        return (self.message.timeout or 0.0) / 2.0

    def should_beat(self) -> bool:
        return self.message.timeout is not None

    def cancel(self) -> None:
        self.action.cancel()
        self.heartbeat.cancel()

    def __await__(self) -> Generator[None, None, Tuple[Set[asyncio.Future], Set[asyncio.Future]]]:
        return asyncio.wait((self.action, self.heartbeat), return_when=asyncio.FIRST_EXCEPTION).__await__()


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

        self.tasks: Dict[TaskID, Task] = {}
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
        self.shutdown_event = asyncio.Event()

        # The lock is used to ensure that only a single tox subprocess
        # can run at any given time.
        self.lock = asyncio.Lock()

        # Tracks pending coroutines
        self.tasks = {}
        self.socket = self.zctx.socket(zmq.ROUTER)
        self.socket.bind(self.uri)

        log.info("Running server at {self.uri}")
        log.info("^C to exit")
        try:
            self._processor = asyncio.create_task(self.process())

            with interrupt_handler(signal.SIGTERM, self.shutdown, oneshot=True), interrupt_handler(
                signal.SIGINT, self.shutdown, oneshot=True
            ):
                await self.shutdown_event.wait()
                await self.drain()
        except asyncio.CancelledError:
            log.info("Server cancelled")
        except BaseException:
            log.exception("Server loop error")
            raise
        finally:
            self._processor.cancel()
            self.socket.close()
        log.debug("Server is done.")

    async def shutdown(self) -> None:
        """Shutdown this server, cancelling processing tasks"""
        self.shutdown_event.set()
        self._processor.cancel()

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
        while not self.shutdown_event.is_set():
            with contextlib.suppress(ProtocolError):
                msg = await self.recv()
                if (msg.command, msg.identifier) in self.tasks:
                    await self.send(
                        msg.respond(
                            Command.ERR, args={"message": "A task has already started.", "command": msg.command.name}
                        )
                    )
                else:
                    task = Task(msg, self)
                    self.tasks[task.id] = task

    async def run_heartbeat(self, task: Task) -> None:
        if not task.should_beat():
            return

        log.debug(f"Starting heartbeat: {task}")
        with contextlib.suppress(asyncio.CancelledError):
            while not self.shutdown_event.is_set():
                try:
                    period = await task.beat(self.socket)
                    await asyncio.wait((self.shutdown_event.wait(),), timeout=period)
                except Exception:
                    log.exception("Heartbeat exception")
                    raise
        log.debug(f"Ending heartbeat: {task}")

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

    async def handle(self, task: Task) -> None:
        """Handle a single tox-server protocol message, and respond.

        This coroutine is designed to be run as its own task, and will
        respond on the socket when appropriate. It delegates command
        work to methods with names of the form ``handle_<command>`` where
        ``<command>`` is the name of the command enum in lower case.
        """
        try:
            handler = getattr(self, f"handle_{task.message.command.name.lower()}")
            await handler(task)
        except asyncio.CancelledError:
            log.debug(f"Notifying client of cancellation {task.message!r}")
            err = task.message.respond(
                command=Command.ERR, args={"message": f"Command {task.message.command.name} was cancelled."}
            )
            await self.send(err)
        except Exception as e:
            log.exception("Error in handler")
            err = task.message.respond(command=Command.ERR, args={"message": str(e)})
            await self.send(err)
        finally:
            self.tasks.pop(task.id, None)
            task.cancel()
            log.debug(f"Finished handling {task!r}")

    async def handle_interrupt(self, task: Task) -> None:
        """Handles an interrupt message

        If the client has sent a message and that message is processing,
        it will be cancelled. Any task can be cancelled, but this is really
        only useful for cancelling RUN tasks.
        """
        task.start()
        try:
            target = Command[task.message.args["command"]]
        except KeyError:
            raise ValueError(f"Unknown command {task.message.args['command']}")

        target_task = self.tasks.get((target, task.message.identifier), None)
        if target_task:
            log.debug(f"Interrupting {task!r}")
            target_task.cancel()
        else:
            log.debug(f"Task to interrupt not found: {task!r}")
            await self.send(task.message.respond(Command.ERR, {"message": "Could not find task"}))

    async def handle_cancel(self, task: Task) -> None:
        """Handles a cancel message to cancel all commands of a specific type

        If the client has any commands of this type, they will be cancelled
        """
        task.start()

        try:
            target = Command[task.message.args["command"]]
        except KeyError:
            raise ValueError(f"Unknown command {task.message.args['command']}")

        # Do this once so we only cancel tasks in progress when this command was sent.
        tasks = [tt for (command, _), tt in self.tasks.items() if command == target]

        for tt in tasks:
            log.debug(f"Cancelling {target!r}")
            tt.cancel()

        await self.send(task.message.respond(Command.CANCEL, args={"cancelled": len(tasks)}))

    async def handle_run(self, task: Task) -> None:
        """Handles a run message to start a tox subprocess.

        Runs are controlled by :func:`tox_command`, and locked by the
        server to ensure that only one tox process runs at a time.
        """
        async with self.lock:
            task.start()
            result = await tox_command(task.message.args["tox"], self.socket, task.message, tee=self.tee)
        await self.send(task.message.respond(Command.RUN, {"returncode": result.returncode, "args": result.args}))

    async def handle_quit(self, task: Task) -> None:
        """Handles a quit message to gracefully shut down.

        Quit starts a graceful shutdown process for the server.
        """
        task.start()
        self.shutdown_event.set()
        log.debug("Requesting shutdown")
        await self.send(task.message.respond(Command.QUIT, "DONE"))

    async def handle_ping(self, task: Task) -> None:
        """Handles a ping message to check the server liveliness"""
        task.start()
        await self.send(task.message.respond(Command.PING, {"time": time.time(), "tasks": len(self.tasks)}))


@click.command()
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

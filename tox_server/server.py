import asyncio
import contextlib
import logging
import signal
import time
from typing import Dict
from typing import Optional
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
        self.beats: Dict[Tuple[Command, Tuple[bytes, ...]], asyncio.Future] = {}
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
        self.beats = {}
        self.socket = self.zctx.socket(zmq.ROUTER)
        self.socket.bind(self.uri)

        log.info(f"Running server at {self.uri}")
        log.info(f"^C to exit")
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
        log.debug(f"Server is done.")

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
        if self.beats:
            await asyncio.wait(self.beats.values())

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
                    if msg.timeout:
                        self.beats[(msg.command, msg.identifier)] = asyncio.create_task(
                            self.run_heartbeat(msg, msg.timeout / 2.0)
                        )
                    self.tasks[(msg.command, msg.identifier)] = asyncio.create_task(self.handle(msg))

    async def run_heartbeat(self, template: Message, period: float) -> None:
        log.debug(f"Starting heartbeat: {template}")
        with contextlib.suppress(asyncio.CancelledError):
            while not self.shutdown_event.is_set():
                msg = template.respond(Command.HEARTBEAT, args={"now": time.time()})
                await msg.send(self.socket)
                await asyncio.wait((self.shutdown_event.wait(),), timeout=period)
        log.debug(f"Ending heartbeat: {template}")

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
        except asyncio.CancelledError:
            log.debug(f"Notifying client of cancellation {msg!r}")
            err = msg.respond(command=Command.ERR, args={"message": f"Command {msg.command.name} was cancelled."})
            await self.send(err)
        except Exception as e:
            log.exception("Error in handler")
            err = msg.respond(command=Command.ERR, args={"message": str(e)})
            await self.send(err)
        finally:
            self.tasks.pop((msg.command, msg.identifier), None)
            beat = self.beats.pop((msg.command, msg.identifier), None)
            if beat:
                beat.cancel()
            log.debug(f"Finished handling {msg!r}")

    async def handle_interrupt(self, msg: Message) -> None:
        """Handles an interrupt message

        If the client has sent a message and that message is processing,
        it will be cancelled. Any task can be cancelled, but this is really
        only useful for cancelling RUN tasks.
        """
        try:
            target = Command[msg.args["command"]]
        except KeyError:
            raise ValueError(f"Unknown command {msg.args['command']}")

        task = self.tasks.get((target, msg.identifier), None)
        if task:
            log.debug(f"Interrupting {task!r}")
            task.cancel()
        else:
            log.debug(f"Task to interrupt not found: {msg!r}")
            await self.send(msg.respond(Command.ERR, {"message": "Could not find task"}))

    async def handle_cancel(self, msg: Message) -> None:
        """Handles a cancel message to cancel all commands of a specific type

        If the client has any commands of this type, they will be cancelled
        """
        try:
            target = Command[msg.args["command"]]
        except KeyError:
            raise ValueError(f"Unknown command {msg.args['command']}")

        # Do this once so we only cancel tasks in progress when this command was sent.
        tasks = [task for (command, _), task in self.tasks.items() if command == target]

        for task in tasks:
            log.debug(f"Cancelling {task!r}")
            task.cancel()

        await self.send(msg.respond(Command.CANCEL, args={"cancelled": len(tasks)}))

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
        self.shutdown_event.set()
        log.debug("Requesting shutdown")
        await self.send(msg.respond(Command.QUIT, "DONE"))

    async def handle_ping(self, msg: Message) -> None:
        """Handles a ping message to check the server liveliness"""
        await self.send(msg.respond(Command.PING, {"time": time.time(), "tasks": len(self.tasks)}))


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

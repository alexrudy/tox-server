import asyncio
import base64
import functools
import logging
import signal
import subprocess
import sys
from typing import Optional
from typing import Tuple

import click
import zmq.asyncio

from .interrupt import interrupt_handler
from .process import Stream
from .protocol import Command
from .protocol import Message

log = logging.getLogger(__name__)


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


async def send_interrupt(socket: zmq.asyncio.Socket, message: Message) -> None:
    """Helper function to send an interrupt message using the provided socket"""
    interrupt = message.interrupt().for_dealer()
    log.debug(f"Sending interrupt: {interrupt!r}")
    click.echo("", err=True)
    click.echo(f"Cancelling {command_name()} with the server. ^C again to exit.", err=True)
    await interrupt.send(socket)


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

    state = "QUEUED"
    has_printed_output = False

    with interrupt_handler(signal.SIGINT, sigint_callback, oneshot=True), client:

        await message.for_dealer().send(client)

        while True:
            response = await asyncio.wait_for(Message.recv(client), timeout=timeout)
            if response.command == Command.OUTPUT:
                has_printed_output = True
                Stream[response.args["stream"]].fwrite(base64.b85decode(response.args["data"]))
            elif response.command == Command.HEARTBEAT:
                # Ensures that we don't timeout above.
                state = response.args["state"]
            else:
                # Note: this assumes that OUTPUT is the only command which shouldn't end
                # the await loop above, which might not be true...
                break

            if not has_printed_output and state == "QUEUED":
                has_printed_output = True
                click.echo(f"Command {message.command.name} is queued")

    return response


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
    except BaseException:
        log.exception("Unhandled exception in asyncio loop")
        raise
    else:
        if response.command == Command.ERR:
            log.error(f"Error in command: {response!r}")
            click.echo(response.args.get("message", repr(response.args)), err=True)
            click.echo(f"Command {command_name()} error!", err=True)
            raise SystemExit(1)
        return response


@click.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.pass_context
def run(ctx: click.Context) -> None:
    """
    Run a tox command on the server.

    All arguments are forwarded to `tox` on the host machine.
    """
    tox_args = unparse_arguments(tuple(ctx.args))
    cfg = ctx.find_object(dict)

    message = Message(command=Command.RUN, args={"tox": tox_args}, timeout=cfg["timeout"])
    response = run_client(cfg["uri"], message, timeout=cfg["timeout"])

    proc: subprocess.CompletedProcess = subprocess.CompletedProcess(
        args=response.args["args"], returncode=response.args["returncode"]
    )
    if proc.returncode == 0:
        click.echo(f"[{click.style('DONE', fg='green')}] passed")
    else:
        click.echo(f"[{click.style('FAIL', fg='red')}] failed")
    sys.exit(proc.returncode)


@click.command()
@click.pass_context
def quit(ctx: click.Context) -> None:
    """
    Quit the server.
    """
    cfg = ctx.find_object(dict)

    message = Message(command=Command.QUIT, args=None)
    response = run_client(cfg["uri"], message, timeout=cfg["timeout"])
    click.echo(response.args)


@click.command()
@click.option("--command", "-c", type=str, help="Command to cancel, default is 'run'.", default="RUN")
@click.pass_context
def cancel(ctx: click.Context, command: str) -> None:
    """
    Cancel all commands of a particular flavor on the server.
    """
    cfg = ctx.find_object(dict)

    message = Message(command=Command.CANCEL, args={"command": command.upper()})
    response = run_client(cfg["uri"], message, timeout=cfg["timeout"])
    click.echo(response.args)


@click.command()
@click.pass_context
def ping(ctx: click.Context) -> None:
    """
    Ping the server, to check if it is alive.
    """
    cfg = ctx.find_object(dict)
    message = Message(command=Command.PING, args=None)
    response = run_client(cfg["uri"], message, timeout=cfg["timeout"])
    click.echo(response.args)


def init_click_group(group: click.Group) -> None:
    group.add_command(run)
    group.add_command(ping)
    group.add_command(quit)
    group.add_command(cancel)

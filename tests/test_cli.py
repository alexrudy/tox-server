import contextlib
import dataclasses as dc
import logging
import multiprocessing as mp
import os
import signal
import sys
from typing import Any
from typing import Iterator
from typing import List
from typing import Optional

import click.testing
import zmq.asyncio

from .helpers import mark_asyncio_timeout
from tox_server import cli
from tox_server.protocol import Command
from tox_server.protocol import Message

log = logging.getLogger(__name__)


@contextlib.contextmanager
def click_in_process(
    args: List[str],
    exit_code: int = 0,
    timeout: float = 0.1,
    check_exit_code: bool = True,
    check_output: Optional[str] = None,
) -> Iterator[mp.Process]:
    (recv, send) = mp.Pipe()
    started = mp.Event()
    proc = mp.Process(target=_click_process_target, args=(args, send, started), name="cli-process")
    proc.start()
    try:
        started.wait(timeout)

        yield proc

        if recv.poll(timeout):
            result = recv.recv()
            if check_exit_code:
                assert result.exit_code == exit_code
            if check_output:
                assert check_output in result.output
        else:
            raise ValueError("No result recieved from command {args!r}")

        proc.join(timeout)
    finally:
        proc.terminate()


@dc.dataclass
class ProcessResult:
    exit_code: int
    output: str


def _click_process_target(args: List[str], chan: Any, event: Any) -> None:
    try:
        runner = click.testing.CliRunner()
        event.set()
        sys.argv = [sys.argv[0]] + args
        result = runner.invoke(cli.main, args=args)
        chan.send(ProcessResult(result.exit_code, result.output))
    except BaseException as e:
        print(e)
        raise
    finally:
        chan.send(ProcessResult(-1, "Process kinda failed"))


@contextlib.contextmanager
def server_in_process(port: int, exit_code: int = 0, check_exit_code: bool = True) -> Iterator[mp.Process]:
    args = [f"-p{port:d}", "-b127.0.0.1", "serve"]
    with click_in_process(args, exit_code=exit_code, check_exit_code=check_exit_code) as proc:
        yield proc


def test_cli_quit(unused_tcp_port: int) -> None:

    with server_in_process(unused_tcp_port):

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0

        assert "DONE" in result.output


def test_cli_ping(unused_tcp_port: int) -> None:

    with server_in_process(unused_tcp_port):

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "ping"])
        assert result.exit_code == 0

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0


def test_cli_run_help(unused_tcp_port: int) -> None:

    with server_in_process(unused_tcp_port):

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "run", "--", "--help"])
        assert result.exit_code == 0

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0


def test_cli_run_unknown_argument(unused_tcp_port: int) -> None:

    with server_in_process(unused_tcp_port):

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "run", "--", "--foo"])
        assert result.exit_code == 2

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0


@mark_asyncio_timeout(2)
async def test_cli_interrupt(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "quit"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args, exit_code=3) as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        # First signal sends CANCEL
        os.kill(proc.pid, signal.SIGINT)
        # We get the cancel message, but we won't
        # respond. This again indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        assert proc.is_alive()

        # Second signal interrupts process
        os.kill(proc.pid, signal.SIGINT)
        proc.join(0.1)
        assert not proc.is_alive()


@mark_asyncio_timeout(1)
async def test_cli_timeout(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "-t0.1", "quit"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args, exit_code=2, timeout=0.2) as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        # The context manager will now assert that we got
        # exit_code == 2, which indicates a timeout.


@mark_asyncio_timeout(1)
async def test_cli_error(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "quit"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args, exit_code=1) as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        response = msg.respond(command=Command.ERR, args={"message": "An error occured"})
        await response.send(server)

        # The context manager will now assert that we got
        # exit_code == 1, which indicates a protocol error.


@mark_asyncio_timeout(1)
async def test_cli_cancel(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "cancel"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args, exit_code=0, timeout=0.2) as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        await msg.respond(Command.CANCEL, args={"cancelled": 0}).send(server)

        # The context manager will now assert that we got
        # exit_code == 0, which indicates success.


@mark_asyncio_timeout(1)
async def test_cli_argparse(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "run", "--foo", "--", "--arg-here"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args, exit_code=0) as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        assert msg.args["tox"] == ["--foo", "--", "--arg-here"]

        response = msg.respond(command=Command.RUN, args={"returncode": 0, "args": msg.args["tox"]})
        await response.send(server)

        # The context manager will now assert that we got
        # exit_code == 0, which indicates a successful exit.


@mark_asyncio_timeout(1)
async def test_cli_slow_message(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "run", "--foo", "--", "--arg-here"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args, exit_code=0, check_output="Command RUN is queued") as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        assert msg.args["tox"] == ["--foo", "--", "--arg-here"]

        heartbeat = msg.respond(command=Command.HEARTBEAT, args={"state": "QUEUED"})
        await heartbeat.send(server)

        response = msg.respond(command=Command.RUN, args={"returncode": 0, "args": msg.args["tox"]})
        await response.send(server)

        # The context manager will now assert that we got
        # exit_code == 0, which indicates a successful exit.


def test_server_cli_interrupt(unused_tcp_port: int) -> None:
    """Test how the server process responds to interruptions"""

    with server_in_process(unused_tcp_port, exit_code=0) as proc:
        # Ensure we can roundtrip with the server before killing it.
        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "ping"])
        assert result.exit_code == 0

        assert proc.pid is not None
        os.kill(proc.pid, signal.SIGINT)


def test_server_cli_term(unused_tcp_port: int) -> None:
    """Test how the server process responds to interruptions"""

    with server_in_process(unused_tcp_port, exit_code=0) as proc:

        # Ensure we can roundtrip with the server before killing it.
        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "ping"])
        assert result.exit_code == 0

        assert proc.pid is not None
        os.kill(proc.pid, signal.SIGTERM)


def test_cli_loglevel() -> None:

    runner = click.testing.CliRunner()

    @click.command()
    @click.option("--level", type=cli.LogLevelParamType(), default=None)
    def cmd(level: int) -> None:
        print(level)

    result = runner.invoke(cmd, ["--level", "INFO"])
    assert result.exit_code == 0
    assert int(result.output.strip()) == logging.INFO

    result = runner.invoke(cmd, ["--level", "15"])
    assert result.exit_code == 0
    assert int(result.output.strip()) == 15

    result = runner.invoke(cmd, ["--level", "foo"])
    assert result.exit_code == 2

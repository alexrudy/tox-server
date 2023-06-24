import contextlib
import dataclasses as dc
import logging
import multiprocessing as mp
import os
import signal
import sys
import warnings
from typing import Any
from typing import Dict
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


@dc.dataclass
class ProcessResult:
    exit_code: int
    output: str


@dc.dataclass
class ClickProcess:
    proc: mp.Process

    result: Optional[ProcessResult] = None

    def finished(self) -> ProcessResult:
        assert self.result is not None
        return self.result

    @property
    def pid(self) -> Optional[int]:
        return self.proc.pid

    def is_alive(self) -> bool:
        return self.proc.is_alive()

    def join(self, timeout: Optional[float]) -> None:
        if not timeout:
            warnings.warn("proc.join() called without timeout")
        self.proc.join(timeout=timeout)


@contextlib.contextmanager
def click_in_process(
    args: List[str], timeout: float = 0.1, env: Optional[Dict[str, str]] = None
) -> Iterator[ClickProcess]:
    (recv, send) = mp.Pipe()
    started = mp.Event()
    proc = mp.Process(target=_click_process_target, args=(args, send, started, env), name="cli-process")
    proc.start()
    client = ClickProcess(proc=proc)
    try:
        started.wait(timeout)

        yield client

        if recv.poll(timeout):
            client.result = recv.recv()
        else:
            raise ValueError(f"No result recieved from command {args!r}")

        proc.join(timeout)
    finally:
        proc.terminate()


def _click_process_target(args: List[str], chan: Any, event: Any, env: Optional[Dict[str, str]] = None) -> None:
    try:
        runner = click.testing.CliRunner()
        event.set()
        sys.argv = [sys.argv[0]] + args
        result = runner.invoke(cli.main, args=args, env=env)
        chan.send(ProcessResult(result.exit_code, result.output))
    except BaseException as e:
        print(e)
        raise
    finally:
        chan.send(ProcessResult(-1, "Process kinda failed"))


@contextlib.contextmanager
def server_in_process(port: int) -> Iterator[ClickProcess]:
    args = [f"-p{port:d}", "-b127.0.0.1", "serve"]
    with click_in_process(args) as proc:
        yield proc


def test_cli_quit(unused_tcp_port: int) -> None:
    with server_in_process(unused_tcp_port) as server:
        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0

        assert "DONE" in result.output

    assert server.finished().exit_code == 0


def test_cli_ping(unused_tcp_port: int) -> None:
    with server_in_process(unused_tcp_port) as server:
        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "ping"])
        assert result.exit_code == 0

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0

    assert server.finished().exit_code == 0


def test_cli_run_help(unused_tcp_port: int) -> None:
    with server_in_process(unused_tcp_port) as server:
        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "run", "--", "--help"])
        assert result.exit_code == 0

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0
    assert server.finished().exit_code == 0


def test_cli_run_unknown_argument(unused_tcp_port: int) -> None:
    with server_in_process(unused_tcp_port) as server:
        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "run", "--", "--foo"])
        assert result.exit_code == 2

        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0
    assert server.finished().exit_code == 0


@mark_asyncio_timeout(2)
async def test_cli_interrupt(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "quit"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args) as proc:
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

    result = proc.finished()
    print(result.output)
    assert result.exit_code == 3


@mark_asyncio_timeout(1)
async def test_cli_timeout(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "-t0.1", "quit"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args, timeout=0.2) as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

    # exit_code == 2, which indicates a timeout.
    assert proc.finished().exit_code == 2


@mark_asyncio_timeout(1)
async def test_cli_error(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "quit"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args) as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        response = msg.respond(command=Command.ERR, args={"message": "An error occured"})
        await response.send(server)

    # exit_code == 1, which indicates a protocol error.
    assert proc.finished().exit_code == 1


@mark_asyncio_timeout(1)
async def test_cli_cancel(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "cancel"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args, timeout=0.2) as proc:
        assert proc.pid is not None

        # We get the message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        await msg.respond(Command.CANCEL, args={"cancelled": 0}).send(server)

    # exit_code == 0, which indicates success.
    assert proc.finished().exit_code == 0


@mark_asyncio_timeout(1)
async def test_cli_argparse(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "run", "--foo", "--", "--arg-here"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args) as proc:
        assert proc.pid is not None

        # We get the message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        assert msg.args["tox"] == ["--foo", "--", "--arg-here"]

        response = msg.respond(command=Command.RUN, args={"returncode": 0, "args": msg.args["tox"]})
        await response.send(server)

    # exit_code == 0, which indicates a successful exit.
    assert proc.finished().exit_code == 0


@mark_asyncio_timeout(1)
async def test_cli_slow_message(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "run", "--foo", "--", "--arg-here"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    env = {"_TOX_SERVER_TIMEOUT_FOR_QUEUE_NOTIFICATION": "0.0"}

    with server, click_in_process(args, env=env) as proc:
        assert proc.pid is not None

        # We get the message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        assert msg.args["tox"] == ["--foo", "--", "--arg-here"]

        heartbeat = msg.respond(command=Command.HEARTBEAT, args={"state": "QUEUED"})
        await heartbeat.send(server)

        response = msg.respond(command=Command.RUN, args={"returncode": 0, "args": msg.args["tox"]})
        await response.send(server)

    # exit_code == 0, which indicates a successful exit.
    assert proc.finished().exit_code == 0
    assert "Command RUN is queued" in proc.finished().output


@mark_asyncio_timeout(1)
async def test_cli_no_state(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "run", "--foo", "--", "--arg-here"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args) as proc:
        assert proc.pid is not None

        # We get the message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        assert msg.args["tox"] == ["--foo", "--", "--arg-here"]

        heartbeat = msg.respond(command=Command.HEARTBEAT, args={})
        await heartbeat.send(server)

        response = msg.respond(command=Command.RUN, args={"returncode": 0, "args": msg.args["tox"]})
        await response.send(server)

    # exit_code == 1, which indicates a protocol error.
    assert proc.finished().exit_code == 1


@mark_asyncio_timeout(1)
async def test_cli_old_protocol(unused_tcp_port: int, zctx: zmq.asyncio.Context) -> None:
    args = [f"-p{unused_tcp_port:d}", "-hlocalhost", "-ldebug", "run", "--foo", "--", "--arg-here"]

    server = zctx.socket(zmq.ROUTER)
    server.setsockopt(zmq.LINGER, 0)
    server.bind(f"tcp://127.0.0.1:{unused_tcp_port:d}")

    with server, click_in_process(args) as proc:
        assert proc.pid is not None

        # We get the message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await Message.recv(server)
        log.debug(repr(msg))

        assert msg.args["tox"] == ["--foo", "--", "--arg-here"]

        heartbeat = msg.respond(command=Command.HEARTBEAT, args={})
        await heartbeat.send(server, version=None)

        response = msg.respond(command=Command.RUN, args={"returncode": 0, "args": msg.args["tox"]})

        await response.send(server)

    # exit_code == 0, which indicates a successful exit.
    assert proc.finished().exit_code == 0


def test_server_cli_interrupt(unused_tcp_port: int) -> None:
    """Test how the server process responds to interruptions"""

    with server_in_process(unused_tcp_port) as proc:
        # Ensure we can roundtrip with the server before killing it.
        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "ping"])
        assert result.exit_code == 0

        assert proc.pid is not None
        os.kill(proc.pid, signal.SIGINT)

    assert proc.finished().exit_code == 0


def test_server_cli_term(unused_tcp_port: int) -> None:
    """Test how the server process responds to interruptions"""

    with server_in_process(unused_tcp_port) as proc:
        # Ensure we can roundtrip with the server before killing it.
        runner = click.testing.CliRunner()
        result = runner.invoke(cli.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "ping"])
        assert result.exit_code == 0

        assert proc.pid is not None
        os.kill(proc.pid, signal.SIGTERM)

    assert proc.finished().exit_code == 0


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


def test_cli_version() -> None:
    runner = click.testing.CliRunner()

    result = runner.invoke(cli.main, "--version")
    assert result.exit_code == 0
    assert "version" in result.output

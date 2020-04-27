import asyncio
import contextlib
import dataclasses as dc
import functools
import json
import multiprocessing as mp
import os
import signal
import sys
import unittest.mock as mock
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import Iterator
from typing import List
from typing import Tuple
from typing import TypeVar

import click.testing
import pytest
import zmq.asyncio

import tox_server as ts

if sys.version_info < (3, 8):
    # On python 3.7 and earlier, the builtin mock doesn't support AsyncMock,
    # so we grab the backported version from pypi
    import mock  # type: ignore  # noqa: F811

F = TypeVar("F", bound=Callable)


def mark_asyncio_timeout(timeout: int) -> Callable:
    def _inner(f: F) -> F:
        @pytest.mark.asyncio
        @functools.wraps(f)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await asyncio.wait_for(f(*args, **kwargs), timeout=timeout)

        return wrapper

    return _inner


async def ensure_task_finished(task: asyncio.Future) -> None:
    await asyncio.wait((task,), timeout=0.1)
    exc = task.exception()
    if exc is not None:
        raise exc
    return task.done()


@pytest.mark.parametrize("stream", [ts.Stream.STDERR, ts.Stream.STDOUT])
def test_local_streams(stream: ts.Stream) -> None:

    assert hasattr(stream._get_stream(), "write")
    assert hasattr(stream._get_stream(), "flush")


@pytest.fixture()
def mock_publisher() -> zmq.asyncio.Socket:
    async def publish(message: List[bytes], flags: int = 0) -> None:
        # Black-hole sent messages
        return

    socket = mock.MagicMock(zmq.asyncio.Socket)
    socket.send_multipart.side_effect = publish

    return socket


@pytest.fixture()
def mock_stream() -> asyncio.StreamReader:
    first = True

    async def reader(n: int = 1) -> bytes:
        nonlocal first
        if first:
            first = False
            return b"hello"
        else:
            return b""

    stream = mock.AsyncMock(asyncio.StreamReader)  # type: ignore
    stream.read.side_effect = reader
    return stream


@pytest.fixture()
def zctx() -> zmq.asyncio.Context:

    context = zmq.asyncio.Context()
    yield context
    context.destroy(linger=0)


@mark_asyncio_timeout(1)
@pytest.mark.parametrize("tee", [True, False])
async def test_send_output(tee: bool, mock_stream: asyncio.StreamReader, mock_publisher: zmq.asyncio.Socket) -> None:

    message = ts.Message(command=ts.Command.RUN, args=None)
    task = asyncio.create_task(ts.publish_output(mock_stream, mock_publisher, message, ts.Stream.STDOUT, tee=tee))

    await asyncio.sleep(0.1)
    task.cancel()

    await asyncio.wait((task,), timeout=1)

    assert task.cancelled()

    mock_stream.read.assert_called()  # type: ignore
    mock_publisher.send_multipart.assert_called()


@dc.dataclass
class URI:
    connect: str
    bind: str


@pytest.fixture(params=["inproc", "tcp"])
def protocol(request: Any) -> str:
    return request.param


@pytest.fixture
def uri(protocol: str, unused_tcp_port: int) -> URI:
    if protocol == "inproc":
        return URI("inproc://control", "inproc://control")
    elif protocol == "tcp":
        return URI(f"tcp://localhost:{unused_tcp_port}", f"tcp://127.0.0.1:{unused_tcp_port}")
    raise ValueError(protocol)


@dc.dataclass
class SubprocessManager:
    returncode: asyncio.Future
    proc: mock.Mock
    create_subprocess_shell: mock.Mock


@pytest.fixture
async def process(monkeypatch: Any, event_loop: Any) -> SubprocessManager:

    returncode: asyncio.Future[int] = event_loop.create_future()

    async def proc_wait() -> int:
        ts.log.debug("Waiting for returncode")
        rc = await asyncio.wait_for(asyncio.shield(returncode), timeout=1)
        ts.log.debug(f"Got returncode: {rc}")
        return rc

    def proc_terminate() -> None:
        if not returncode.done():
            returncode.set_result(-1)

    css = mock.AsyncMock()  # type: ignore
    css.return_value = proc = mock.AsyncMock(spec=asyncio.subprocess.Process)  # type: ignore
    proc.stdout = mock.Mock()
    proc.stderr = mock.Mock()
    proc.wait.side_effect = proc_wait
    proc.terminate.side_effect = proc_terminate
    monkeypatch.setattr(asyncio.subprocess, "create_subprocess_shell", css)
    return SubprocessManager(returncode, proc, css)


@contextlib.asynccontextmanager
async def run_task(aw: Awaitable[Any]) -> AsyncIterator[asyncio.Future]:
    task = asyncio.create_task(aw)
    try:
        yield task
    finally:
        task.cancel()
        assert await ensure_task_finished(task), f"Task {task!r} did not finish"


@contextlib.asynccontextmanager
async def run_server(uri: str, zctx: zmq.asyncio.Context) -> AsyncIterator[asyncio.Future]:

    server = ts.Server(uri, zctx=zctx)
    async with run_task(server.serve_forever()) as server_task:
        yield server_task


@pytest.fixture
async def server(process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context) -> AsyncIterator[asyncio.Future]:
    async with run_server(uri.bind, zctx) as server_task:
        yield server_task


async def check_command(
    command: str, args: Any, uri: URI, zctx: zmq.asyncio.Context, process: SubprocessManager
) -> Tuple[ts.Message, bool]:

    async with run_server(uri.bind, zctx) as server_task:

        message = ts.Message(command=ts.Command[command], args=args)
        client_task = asyncio.create_task(ts.client(uri.connect, message, zctx=zctx, timeout=1))
        process.returncode.set_result(0)
        (done, pending) = await asyncio.wait((server_task, client_task), timeout=0.2, return_when=asyncio.ALL_COMPLETED)

    rv = client_task.result()
    assert client_task in done, "Client task did not finish"

    return rv, (server_task in done)


@mark_asyncio_timeout(1)
async def test_run_command(process: SubprocessManager, mock_publisher: zmq.asyncio.Socket) -> None:
    msg = ts.Message(command=ts.Command.RUN, args=None)

    async with run_task(ts.tox_command(["foo"], mock_publisher, msg)) as task:

        await asyncio.wait((task,), timeout=0.2)

        ts.log.debug(f"Cancelling tox future {task!r}")
        task.cancel()

        rv = await task

    process.proc.terminate.assert_called()
    assert rv.returncode == -1


IGNORE = object()


@mark_asyncio_timeout(1)
@pytest.mark.parametrize(
    "command, args, rcommand, rargs",
    [
        ("QUIT", None, "QUIT", "DONE"),
        ("PING", None, "PING", IGNORE),
        ("RUN", {"tox": ["foo"], "channel": "bar"}, "RUN", {"returncode": 0, "args": ["foo"]}),
    ],
    ids=["QUIT", "PING", "RUN"],
)
async def test_serve(
    command: str, args: Any, rcommand: str, rargs: Any, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:
    rv, finished = await check_command(command, args, uri, zctx, process)
    assert finished == (command == "QUIT")
    assert rv.command.name == rcommand
    if rargs is not IGNORE:
        assert rv.args == rargs


@mark_asyncio_timeout(1)
async def test_client_run(process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context) -> None:

    async with run_server(uri.bind, zctx) as server_task:

        msg = ts.Message(command=ts.Command.RUN, args={"tox": []})
        async with run_task(ts.client(uri.connect, msg, zctx=zctx)) as client_task:

            process.returncode.set_result(0)

            (done, pending) = await asyncio.wait(
                (server_task, client_task), timeout=0.2, return_when=asyncio.ALL_COMPLETED
            )

            assert client_task in done
            msg = client_task.result()
            assert msg.command == ts.Command.RUN
            assert msg.args["returncode"] == 0


@mark_asyncio_timeout(1)
async def test_client_interrupt(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)
        msg = ts.Message(command=ts.Command.RUN, args={"tox": []})
        await msg.for_dealer().send(socket)

        msg = ts.Message(command=ts.Command.CANCEL, args=None)
        await msg.for_dealer().send(socket)

        process.returncode.set_result(5)

        response = await ts.Message.recv(socket)

    assert response.command == ts.Command.RUN
    assert response.args["returncode"] == 5


@mark_asyncio_timeout(1)
async def test_client_interrupt_finished_task(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)
        msg = ts.Message(command=ts.Command.RUN, args={"tox": []})
        ts.log.debug(f"CSend: {msg}")
        await msg.for_dealer().send(socket)
        process.returncode.set_result(5)

        response = await ts.Message.recv(socket)
        assert response.command == ts.Command.RUN
        assert response.args["returncode"] == 5

        await asyncio.sleep(0.1)

        cancel_msg = ts.Message(command=ts.Command.CANCEL, args=None)
        ts.log.debug(f"CSend: {cancel_msg}")
        await cancel_msg.for_dealer().send(socket)

        response = await ts.Message.recv(socket)
        assert response.command == ts.Command.ERR
        assert response.args["message"] == "Could not find task"


@mark_asyncio_timeout(1)
async def test_client_interrupt_unknown_task(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        msg = ts.Message(command=ts.Command.RUN, args={"tox": []})
        ts.log.debug(f"CSend: {msg}")
        await msg.for_dealer().send(socket)

        msg = ts.Message(command=ts.Command.RUN, args={"tox": []})
        ts.log.debug(f"CSend: {msg}")
        await msg.for_dealer().send(socket)

        response = await ts.Message.recv(socket)
        assert response.command == ts.Command.ERR
        assert response.args["message"] == "A task has already started."
        assert response.args["command"] == "RUN"

        msg = ts.Message(command=ts.Command.QUIT, args=None)
        ts.log.debug(f"CSend: {msg}")
        await msg.for_dealer().send(socket)
        process.returncode.set_result(2)
        response = await ts.Message.recv(socket)


@mark_asyncio_timeout(1)
async def test_client_repeat_task(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        cancel_msg = ts.Message(command=ts.Command.CANCEL, args=None)
        ts.log.debug(f"CSend: {cancel_msg}")
        await cancel_msg.for_dealer().send(socket)

        response = await ts.Message.recv(socket)
        assert response.command == ts.Command.ERR
        assert response.args["message"] == "Could not find task"


@mark_asyncio_timeout(1)
async def test_server_protocol_error(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        await socket.send_multipart([b"", b"{data"])
        msg = await ts.Message.recv(socket)
        assert msg.command == ts.Command.ERR

        await socket.send_multipart([b""])
        msg = await ts.Message.recv(socket)
        assert msg.command == ts.Command.ERR


@mark_asyncio_timeout(1)
async def test_server_quit_and_drain(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        msg = ts.Message(command=ts.Command.RUN, args={"tox": []})
        await msg.for_dealer().send(socket)

        msg = ts.Message(command=ts.Command.QUIT, args=None)
        await msg.for_dealer().send(socket)
        process.returncode.set_result(2)

        responses = {}
        response = await ts.Message.recv(socket)
        responses[response.command] = response
        response = await ts.Message.recv(socket)
        responses[response.command] = response

        assert responses[ts.Command.RUN].args["returncode"] == 2
        assert ts.Command.QUIT in responses


@contextlib.contextmanager
def click_in_process(args: List[str], exit_code: int = 0, timeout: float = 0.1) -> Iterator[mp.Process]:
    (recv, send) = mp.Pipe()
    started = mp.Event()
    proc = mp.Process(target=_click_process_target, args=(args, send, started), name="cli-process")
    proc.start()
    try:
        started.wait(timeout)

        yield proc

        if recv.poll(timeout):
            result = recv.recv()
            assert result.exit_code == exit_code
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
        result = runner.invoke(ts.main, args=args)
        chan.send(ProcessResult(result.exit_code, result.output))
    except BaseException as e:
        print(e)
        raise
    finally:
        chan.send(ProcessResult(-1, "Process kinda failed"))


@contextlib.contextmanager
def server_in_process(port: int) -> Iterator[mp.Process]:
    args = [f"-p{port:d}", "-b127.0.0.1", "serve"]
    with click_in_process(args) as proc:
        yield proc


def test_cli_quit(unused_tcp_port: int) -> None:

    with server_in_process(unused_tcp_port):

        runner = click.testing.CliRunner()
        result = runner.invoke(ts.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0

        assert "DONE" in result.output


def test_cli_ping(unused_tcp_port: int) -> None:

    with server_in_process(unused_tcp_port):

        runner = click.testing.CliRunner()
        result = runner.invoke(ts.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "ping"])
        assert result.exit_code == 0

        runner = click.testing.CliRunner()
        result = runner.invoke(ts.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0


def test_cli_run_help(unused_tcp_port: int) -> None:

    with server_in_process(unused_tcp_port):

        runner = click.testing.CliRunner()
        result = runner.invoke(ts.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "run", "--", "--help"])
        assert result.exit_code == 0

        runner = click.testing.CliRunner()
        result = runner.invoke(ts.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
        assert result.exit_code == 0


def test_cli_run_unknown_argument(unused_tcp_port: int) -> None:

    with server_in_process(unused_tcp_port):

        runner = click.testing.CliRunner()
        result = runner.invoke(ts.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "run", "--", "--foo"])
        assert result.exit_code == 2

        runner = click.testing.CliRunner()
        result = runner.invoke(ts.main, args=[f"-p{unused_tcp_port:d}", "-hlocalhost", "quit"])
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
        msg = await ts.Message.recv(server)
        ts.log.debug(repr(msg))

        # First signal sends CANCEL
        os.kill(proc.pid, signal.SIGINT)
        # We get the cancel message, but we won't
        # respond. This again indicates that the CLI is running.
        msg = await ts.Message.recv(server)
        ts.log.debug(repr(msg))

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

    with server, click_in_process(args, exit_code=2) as proc:
        assert proc.pid is not None

        # We get the quit message, but we won't
        # respond. This just indicates that the CLI is running.
        msg = await ts.Message.recv(server)
        ts.log.debug(repr(msg))

        # Wait the duration of the timeout, but which may have already
        # happened in the subporcess

        await asyncio.sleep(0.1)
        proc.join(0.1)
        assert not proc.is_alive()


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
        msg = await ts.Message.recv(server)
        ts.log.debug(repr(msg))

        response = msg.respond(command=ts.Command.ERR, args={"message": "An error occured"})
        await response.send(server)

        await asyncio.sleep(0.1)
        proc.join(0.1)
        assert not proc.is_alive()


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
        msg = await ts.Message.recv(server)
        ts.log.debug(repr(msg))

        assert msg.args["tox"] == ["--foo", "--", "--arg-here"]

        response = msg.respond(command=ts.Command.RUN, args={"returncode": 0, "args": msg.args["tox"]})
        await response.send(server)

        await asyncio.sleep(0.1)
        proc.join(0.1)
        assert not proc.is_alive()


class TestMessage:
    def test_parse_empty(self) -> None:

        with pytest.raises(ts.ProtocolFailure):
            ts.Message.parse([])

    def test_parse_invalid_identifiers(self) -> None:

        with pytest.raises(ts.ProtocolFailure):
            ts.Message.parse([b"identifier", b"data"])

    def test_parse_invalid_json(self) -> None:
        with pytest.raises(ts.ProtocolError):
            ts.Message.parse([b"identifier", b"", b"{data"])

    def test_parse_missing_args(self) -> None:

        data = json.dumps({"command": "BAZ"}).encode("utf-8")
        with pytest.raises(ts.ProtocolError) as exc_info:
            ts.Message.parse([b"identifier", b"", data])
        assert exc_info.value.message.args["message"] == "JSON missing key: 'args'"
        assert str(exc_info.value) == "JSON missing key: 'args'"

    def test_parse_invalid_command(self) -> None:
        data = json.dumps({"command": "BAZ", "args": None}).encode("utf-8")
        with pytest.raises(ts.ProtocolError) as exc_info:
            ts.Message.parse([b"identifier", b"", data])
        assert exc_info.value.message.args["message"] == "Unknown command: BAZ"
        assert str(exc_info.value) == "Unknown command: BAZ"

    def test_identifier_default(self) -> None:
        data = json.dumps({"command": "PING", "args": None}).encode("utf-8")
        msg = ts.Message.parse([data])
        assert not msg.identifiers
        assert msg.identifier == (b"",)

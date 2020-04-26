import asyncio
import dataclasses as dc
from typing import Any
from typing import Dict
from typing import Tuple

import mock
import pytest
import zmq.asyncio

import tox_server as ts


@pytest.mark.parametrize("stream", [ts.Stream.STDERR, ts.Stream.STDOUT])
def test_local_streams(stream: ts.Stream) -> None:

    ls = ts.LocalStreams()

    assert stream in ls
    target = ls[stream]
    assert hasattr(target, "flush")
    assert hasattr(target, "write")


@pytest.fixture()
def mock_publisher() -> zmq.asyncio.Socket:
    async def publish(message):
        return

    socket = mock.MagicMock(zmq.asyncio.Socket)
    socket.send_multipart.side_effect = publish

    return socket


@pytest.fixture()
def mock_stream() -> asyncio.StreamReader:
    first = True

    async def reader(n=1):
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
def zctx():

    context = zmq.asyncio.Context()
    yield context
    context.term()


@pytest.mark.asyncio
@pytest.mark.parametrize("tee", [True, False])
async def test_send_output(
    tee: bool, mock_stream: asyncio.StreamReader, mock_publisher: zmq.asyncio.Socket
) -> None:

    message = ts.Message(command=ts.Command.RUN, args=None)
    task = asyncio.ensure_future(
        ts.publish_output(
            mock_stream, mock_publisher, message, ts.Stream.STDOUT, tee=tee
        )
    )

    await asyncio.sleep(0.1)
    task.cancel()

    await asyncio.wait((task,), timeout=1)

    with pytest.raises(asyncio.CancelledError):
        task.result()

    mock_stream.read.assert_called()  # type: ignore
    mock_publisher.send_multipart.assert_called()


@dc.dataclass
class URI:
    connect: str
    bind: str


@pytest.fixture(params=["inproc", "tcp"])
def protocol(request):
    return request.param


@pytest.fixture
def control_uri(protocol):
    if protocol == "inproc":
        return URI("inproc://control", "inproc://control")
    elif protocol == "tcp":
        return URI("tcp://localhost:7890", "tcp://127.0.0.1:7890")
    raise ValueError(protocol)


@pytest.fixture
def output_uri(protocol):
    if protocol == "inproc":
        return URI("inproc://output", "inproc://output")
    elif protocol == "tcp":
        return URI("tcp://localhost:7891", "tcp://127.0.0.1:7891")
    raise ValueError(protocol)


async def send_command(
    control_uri: str, command: str, args: Any, zctx: zmq.asyncio.Context
) -> Dict[str, Any]:
    zctx = zctx or zmq.asyncio.Context.instance()
    zsocket = zctx.socket(zmq.REQ)
    zsocket.connect(control_uri)

    with zsocket:
        await zsocket.send_json({"command": command, "args": args})
        result = await zsocket.recv_json()
    return result


async def check_command(
    command, args, control_uri: URI, zctx: zmq.asyncio.Context
) -> Tuple[ts.Message, bool]:
    server = ts.Server(control_uri.bind, zctx=zctx)
    server_task = asyncio.ensure_future(server.run_forever())

    message = ts.Message(command=ts.Command[command], args=args)

    commander = asyncio.ensure_future(
        ts.single_command(control_uri.connect, message, zctx=zctx, timeout=1)
    )

    (done, pending) = await asyncio.wait(
        (server_task, commander), timeout=0.1, return_when=asyncio.ALL_COMPLETED
    )

    server_task.cancel()
    commander.cancel()

    assert commander in done

    rv = commander.result()

    return rv, (server_task in done)


@pytest.fixture
def create_subprocess_monkey(monkeypatch) -> None:
    css = mock.AsyncMock()  # type: ignore
    css.return_value = proc = mock.AsyncMock()  # type: ignore
    proc.wait = mock.AsyncMock()  # type: ignore
    proc.wait.return_value = 0
    monkeypatch.setattr(asyncio.subprocess, "create_subprocess_shell", css)
    return css


IGNORE = object()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command, args, rcommand, rargs",
    [
        ("QUIT", None, "QUIT", "DONE"),
        ("PING", None, "PONG", IGNORE),
        (
            "RUN",
            {"tox": ["foo"], "channel": "bar"},
            "RUN",
            {"returncode": 0, "args": ["foo"]},
        ),
    ],
    ids=["QUIT", "PING", "RUN"],
)
async def test_serve(
    command, args, rcommand, rargs, create_subprocess_monkey, control_uri, zctx,
) -> None:
    rv, finished = await check_command(command, args, control_uri, zctx)
    assert finished == (command == "QUIT")
    assert rv.command.name == rcommand
    if rargs is not IGNORE:
        assert rv.args == rargs


@pytest.mark.asyncio
async def test_client_run(create_subprocess_monkey, control_uri, zctx) -> None:

    server = ts.Server(control_uri.bind, zctx=zctx)
    server_task = asyncio.ensure_future(server.run_forever())

    run_task = asyncio.ensure_future(
        ts.run_command(control_uri.connect, tox_args=(), zctx=zctx)
    )

    (done, pending) = await asyncio.wait(
        (server_task, run_task), timeout=0.2, return_when=asyncio.ALL_COMPLETED
    )

    server_task.cancel()
    assert run_task in done
    msg = run_task.result()
    assert msg.command == ts.Command.RUN
    assert msg.args["returncode"] == 0

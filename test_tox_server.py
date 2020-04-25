import tox_server as ts
import dataclasses as dc
import pytest
import mock
import zmq
import zmq.asyncio
import asyncio
from typing import Dict
from typing import Optional, Any, Tuple


@pytest.mark.parametrize("stream", [b"STDOUT", b"STDERR"])
def test_local_streams(stream: bytes) -> None:

    ls = ts.LocalStreams()

    assert b"bar" + stream in ls
    stream = ls[b"foo" + stream]
    assert hasattr(stream, "flush")
    assert hasattr(stream, "write")

    with pytest.raises(KeyError):
        ls[b"bazSTDIN"]

    with pytest.raises(KeyError):
        ls[b"bam"]


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

    stream = mock.AsyncMock(asyncio.StreamReader)
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
    task = asyncio.ensure_future(
        ts.publish_output(
            mock_stream, mock_publisher, channel=b"test-channel-STDOUT", tee=tee
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
    command, args, control_uri: URI, output_uri: URI, zctx: zmq.asyncio.Context
) -> Tuple[ts.Message, bool]:
    server = ts.Server(control_uri.bind, output_uri.bind, zctx=zctx)
    server_task = asyncio.ensure_future(server.run_forever())

    message = ts.Message(command=ts.Command[command], args=args)

    commander = asyncio.ensure_future(
        ts.single_command(control_uri.connect, message, zctx=zctx, timeout=1)
    )

    (done, pending) = await asyncio.wait(
        (server_task, commander), timeout=0.2, return_when=asyncio.ALL_COMPLETED
    )

    server_task.cancel()
    await asyncio.wait_for(server_task, timeout=1.0)
    assert commander in done

    rv = commander.result()

    return rv, (server_task in done)


@pytest.fixture
def create_subprocess_monkey(monkeypatch) -> None:
    css = mock.AsyncMock()
    css.return_value = proc = mock.AsyncMock()
    proc.wait = mock.AsyncMock()
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
    command,
    args,
    rcommand,
    rargs,
    create_subprocess_monkey,
    control_uri,
    output_uri,
    zctx,
) -> None:
    rv, finished = await check_command(command, args, control_uri, output_uri, zctx)
    assert finished == (command == "QUIT")
    assert rv.command.name == rcommand
    if rargs is not IGNORE:
        assert rv.args == rargs

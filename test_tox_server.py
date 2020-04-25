import tox_server as ts
import pytest
import mock
import zmq
import zmq.asyncio
import asyncio
from typing import Dict
from typing import Optional, Any


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


@pytest.mark.asyncio
@pytest.mark.parametrize("tee", [True, False])
async def test_send_output(
    tee: bool, mock_stream: asyncio.StreamReader, mock_publisher: zmq.asyncio.Socket
) -> None:
    task = asyncio.create_task(
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


async def send_command(
    control_uri: str,
    command: str,
    args: Any,
    zctx: Optional[zmq.asyncio.Context] = None,
) -> Dict[str, Any]:
    zctx = zctx or zmq.asyncio.Context.instance()
    zsocket = zctx.socket(zmq.REQ)
    zsocket.connect(control_uri)

    with zsocket:
        await zsocket.send_json({"command": command, "args": args})
        result = await zsocket.recv_json()
    return result


async def check_command(command, args):
    zctx = zmq.asyncio.Context.instance()
    server = asyncio.create_task(
        ts.serve_async("inproc://control", "inproc://stream", zctx=zctx)
    )

    commander = asyncio.create_task(
        send_command("inproc://control", command, args, zctx=zctx)
    )

    (done, pending) = await asyncio.wait(
        (server, commander), timeout=0.1, return_when=asyncio.ALL_COMPLETED
    )
    server.cancel()

    assert commander in done

    rv = commander.result()

    return rv, (server in done)


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
        ("BAZ", None, "ERR", {"message": "Unknown command", "command": "BAZ"}),
        (
            "RUN",
            {"tox": ["foo"], "channel": "bar"},
            "RUN",
            {"returncode": 0, "args": ["foo"]},
        ),
    ],
    ids=["QUIT", "PING", "ERR", "RUN"],
)
async def test_serve(command, args, rcommand, rargs, create_subprocess_monkey) -> None:
    rv, finished = await check_command(command, args)
    assert finished == (command == "QUIT")
    assert rv["command"] == rcommand
    if rargs is not IGNORE:
        assert rv["args"] == rargs


@pytest.mark.asyncio
async def test_serve_unknown() -> None:
    zctx = zmq.asyncio.Context.instance()
    task = asyncio.create_task(
        ts.serve_async("inproc://control", "inproc://stream", zctx=zctx)
    )

    commander = asyncio.create_task(
        send_command("inproc://control", "BAZ", None, zctx=zctx)
    )

    (done, pending) = await asyncio.wait(
        (task, commander), timeout=0.1, return_when=asyncio.ALL_COMPLETED
    )
    assert len(pending) == 1
    assert len(done) == 1

    task.cancel()

    rv = commander.result()
    assert rv["command"] == "ERR"
    assert rv["args"]["message"] == "Unknown command"
    assert rv["args"]["command"] == "BAZ"

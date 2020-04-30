import asyncio
import base64
import logging
from typing import List

import pytest
import zmq.asyncio

from .conftest import SubprocessManager
from .helpers import mark_asyncio_timeout
from .helpers import mock
from .helpers import run_task
from tox_server.process import publish_output
from tox_server.process import Stream
from tox_server.process import tox_command
from tox_server.protocol import Command
from tox_server.protocol import Message

log = logging.getLogger(__name__)


@pytest.mark.parametrize("stream", [Stream.STDERR, Stream.STDOUT])
def test_local_streams(stream: Stream) -> None:

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
    reads = 2

    async def read(n: int = 1) -> bytes:
        nonlocal reads
        while reads:
            reads -= 1
            return b"hello"
        else:
            return b""

    def at_eof() -> bool:
        nonlocal reads
        return reads == 0

    stream = mock.AsyncMock(asyncio.StreamReader)  # type: ignore
    stream.read.side_effect = read
    stream.at_eof.side_effect = at_eof
    return stream


@mark_asyncio_timeout(1)
@pytest.mark.parametrize("tee", [True, False])
async def test_send_output(tee: bool, mock_stream: asyncio.StreamReader, zctx: zmq.asyncio.Context) -> None:

    sender = zctx.socket(zmq.PAIR)
    sender.bind("inproc://test-send-output")

    reciever = zctx.socket(zmq.PAIR)
    reciever.connect("inproc://test-send-output")

    message = Message(command=Command.RUN, args=None)
    task = asyncio.create_task(publish_output(mock_stream, sender, message, Stream.STDOUT, tee=tee))

    msg = await reciever.recv_json()

    assert base64.b85decode(msg["args"]["data"].encode("ascii")) == b"hello"

    task.cancel()
    await asyncio.wait((task,), timeout=1)

    mock_stream.read.assert_called()  # type: ignore


@mark_asyncio_timeout(1)
async def test_run_command(process: SubprocessManager, mock_publisher: zmq.asyncio.Socket) -> None:
    msg = Message(command=Command.RUN, args=None)

    async with run_task(tox_command(["foo"], mock_publisher, msg)) as task:

        await asyncio.wait((task,), timeout=0.2)

        log.debug(f"Cancelling tox future {task!r}")
        task.cancel()

        rv = await task

    process.proc.terminate.assert_called()
    assert rv.returncode == -1

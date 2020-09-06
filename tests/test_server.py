import asyncio
import logging
from typing import Any

import pytest
import zmq.asyncio

from .conftest import SubprocessManager
from .conftest import URI
from .helpers import check_command
from .helpers import mark_asyncio_timeout
from .helpers import run_server
from .helpers import run_task
from tox_server.client import client
from tox_server.protocol import Command
from tox_server.protocol import Message

log = logging.getLogger(__name__)

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
async def test_server_run(process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context) -> None:

    async with run_server(uri.bind, zctx) as server_task:

        msg = Message(command=Command.RUN, args={"tox": []})
        async with run_task(client(uri.connect, msg, zctx=zctx)) as client_task:

            process.returncode.set_result(0)

            (done, pending) = await asyncio.wait(
                (server_task, client_task), timeout=0.2, return_when=asyncio.ALL_COMPLETED
            )

            assert client_task in done
            msg = client_task.result()
            assert msg.command == Command.RUN
            assert msg.args["returncode"] == 0


@mark_asyncio_timeout(1)
async def test_server_interrupt(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)
        msg = Message(command=Command.RUN, args={"tox": []})
        await msg.for_dealer().send(socket)

        await msg.interrupt().for_dealer().send(socket)

        process.returncode.set_result(5)

        response = await Message.recv(socket)

    assert response.command == Command.RUN
    assert response.args["returncode"] == 5


@mark_asyncio_timeout(1)
async def test_server_cancel_hang(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    process.state.hang = True
    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)
        msg = Message(command=Command.RUN, args={"tox": []})
        await msg.for_dealer().send(socket)

        cancel = Message(command=Command.CANCEL, args={"command": "RUN"})
        await cancel.for_dealer().send(socket)

        # First let me know the cancel happend
        response = await Message.recv(socket)
        assert response.command == Command.CANCEL

        cancel = Message(command=Command.CANCEL, args={"command": "RUN"})
        await cancel.for_dealer().send(socket)

        # Then let me know the second cancel happend
        response = await Message.recv(socket)
        assert response.command == Command.CANCEL

        # Then let me see the error
        response = await Message.recv(socket)

        assert response.command == Command.ERR
        assert response.args["message"] == "Command RUN was cancelled."


@mark_asyncio_timeout(1)
async def test_server_interrupt_hang(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    process.state.hang = True
    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)
        msg = Message(command=Command.RUN, args={"tox": []})
        await msg.for_dealer().send(socket)

        await msg.interrupt().for_dealer().send(socket)

        await asyncio.sleep(0.1)

        await msg.interrupt().for_dealer().send(socket)

        # Let me see the error
        response = await Message.recv(socket)

        assert response.command == Command.ERR
        assert response.args["message"] == "Command RUN was cancelled."


@mark_asyncio_timeout(1)
async def test_server_cancel_wrong_command(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    process.state.hang = True
    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        command = "FOO"
        with pytest.raises(KeyError):
            Command[command]

        cancel = Message(command=Command.CANCEL, args={"command": command})

        await cancel.for_dealer().send(socket)

        # Show me the error
        response = await Message.recv(socket)
        assert response.command == Command.ERR
        assert response.args["message"] == "Unknown command FOO"


@mark_asyncio_timeout(1)
async def test_server_interrupt_wrong_command(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    process.state.hang = True
    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        command = "FOO"
        with pytest.raises(KeyError):
            Command[command]

        cancel = Message(command=Command.INTERRUPT, args={"command": command})

        await cancel.for_dealer().send(socket)

        # Show me the error
        response = await Message.recv(socket)
        assert response.command == Command.ERR
        assert response.args["message"] == "Unknown command FOO"


@mark_asyncio_timeout(1)
async def test_server_interrupt_finished_task(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)
        msg = Message(command=Command.RUN, args={"tox": []})
        log.debug(f"CSend: {msg}")
        await msg.for_dealer().send(socket)
        process.returncode.set_result(5)

        response = await Message.recv(socket)
        assert response.command == Command.RUN
        assert response.args["returncode"] == 5

        await msg.interrupt().for_dealer().send(socket)

        response = await Message.recv(socket)
        assert response.command == Command.ERR
        assert response.args["message"] == "Could not find task"


@mark_asyncio_timeout(1)
async def test_server_repeat_task(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        msg = Message(command=Command.RUN, args={"tox": []})
        log.debug(f"CSend: {msg}")
        await msg.for_dealer().send(socket)

        msg = Message(command=Command.RUN, args={"tox": []})
        log.debug(f"CSend: {msg}")
        await msg.for_dealer().send(socket)

        response = await Message.recv(socket)
        assert response.command == Command.ERR
        assert response.args["message"] == "A task has already started."
        assert response.args["command"] == "RUN"

        msg = Message(command=Command.QUIT, args=None)
        log.debug(f"CSend: {msg}")
        await msg.for_dealer().send(socket)
        process.returncode.set_result(2)
        response = await Message.recv(socket)


@mark_asyncio_timeout(1)
async def test_server_interrupt_unknown_task(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        interrupt_msg = Message(command=Command.INTERRUPT, args={"command": "RUN"})
        log.debug(f"CSend: {interrupt_msg}")
        await interrupt_msg.for_dealer().send(socket)

        response = await Message.recv(socket)
        assert response.command == Command.ERR
        assert response.args["message"] == "Could not find task"


@mark_asyncio_timeout(1)
async def test_server_protocol_error(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        await socket.send_multipart([b"", b"{data"])
        msg = await Message.recv(socket)
        assert msg.command == Command.ERR

        await socket.send_multipart([b""])
        msg = await Message.recv(socket)
        assert msg.command == Command.ERR


@mark_asyncio_timeout(1)
async def test_server_quit_and_drain(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        msg = Message(command=Command.RUN, args={"tox": []})
        await msg.for_dealer().send(socket)

        msg = Message(command=Command.QUIT, args=None)
        await msg.for_dealer().send(socket)
        process.returncode.set_result(2)

        responses = {}
        response = await Message.recv(socket)
        responses[response.command] = response
        response = await Message.recv(socket)
        responses[response.command] = response

        assert responses[Command.RUN].args["returncode"] == 2
        assert Command.QUIT in responses


@mark_asyncio_timeout(1)
async def test_server_heartbeat(
    server: asyncio.Future, process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context
) -> None:

    with zctx.socket(zmq.DEALER) as socket:
        socket.connect(uri.connect)

        msg = Message(command=Command.RUN, args={"tox": []}, timeout=0.1)
        await msg.for_dealer().send(socket)

        await asyncio.sleep(0.1)

        msg = Message(command=Command.QUIT, args=None)
        await msg.for_dealer().send(socket)

        process.returncode.set_result(2)

        responses = {}

        while True:
            response = await Message.recv(socket)
            responses[response.command] = response
            if response.command == Command.RUN:
                break

        assert responses[Command.RUN].args["returncode"] == 2
        assert Command.QUIT in responses
        assert Command.HEARTBEAT in responses

        assert responses[Command.HEARTBEAT].args["state"] == "STARTED"

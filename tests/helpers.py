import asyncio
import contextlib
import dataclasses as dc
import functools
import logging
import sys
import unittest.mock as mock
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import cast
from typing import Tuple
from typing import TypeVar

import pytest
import zmq.asyncio

import tox_server as ts
import tox_server.process
from tox_server.protocol import Command
from tox_server.protocol import Message
from tox_server.server import Server


if sys.version_info < (3, 8):
    # On python 3.7 and earlier, the builtin mock doesn't support AsyncMock,
    # so we grab the backported version from pypi
    import mock  # type: ignore  # noqa: F811,F401

log = logging.getLogger("tox_server.tests")

F = TypeVar("F", bound=Callable)


def mark_asyncio_timeout(timeout: int) -> Callable:
    def _inner(f: F) -> F:
        @pytest.mark.asyncio
        @functools.wraps(f)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await asyncio.wait_for(f(*args, **kwargs), timeout=timeout)

        return cast(F, wrapper)

    return _inner


@dc.dataclass
class URI:
    connect: str
    bind: str


@dc.dataclass
class SubprocessState:
    hang: bool
    returncode: asyncio.Future


@dc.dataclass
class SubprocessManager:
    proc: mock.Mock
    create_subprocess_shell: mock.Mock
    state: SubprocessState

    @property
    def returncode(self) -> asyncio.Future:
        return self.state.returncode


async def ensure_task_finished(task: asyncio.Future) -> bool:
    await asyncio.wait((task,), timeout=0.1)
    exc = task.exception()
    if exc is not None:
        raise exc
    return task.done()


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

    server = Server(uri, zctx=zctx)
    async with run_task(server.serve_forever()) as server_task:
        yield server_task


async def check_command(
    command: str, args: Any, uri: URI, zctx: zmq.asyncio.Context, process: SubprocessManager
) -> Tuple[Message, bool]:

    async with run_server(uri.bind, zctx) as server_task:

        message = Message(command=Command[command], args=args)
        client_task = asyncio.create_task(ts.client.client(uri.connect, message, zctx=zctx, timeout=1))
        process.returncode.set_result(0)
        (done, pending) = await asyncio.wait((server_task, client_task), timeout=0.2, return_when=asyncio.ALL_COMPLETED)

    rv = client_task.result()
    assert client_task in done, "Client task did not finish"

    return rv, (server_task in done)

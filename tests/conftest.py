import asyncio
import logging
from typing import Any
from typing import AsyncIterator
from typing import List

import pytest
import zmq.asyncio
from pytest import Item
from pytest_mypy import MypyFileItem

from .helpers import mock
from .helpers import run_server
from .helpers import SubprocessManager
from .helpers import SubprocessState
from .helpers import URI

log = logging.getLogger(__name__)


def pytest_collection_modifyitems(items: List[Item]) -> None:
    items[:] = [
        item
        for item in items[:]
        if not (isinstance(item, MypyFileItem) and item.fspath.basename in {"setup.py", "tasks.py"})
    ]


@pytest.fixture()
def zctx() -> zmq.asyncio.Context:

    context = zmq.asyncio.Context()
    yield context
    context.destroy(linger=0)


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


@pytest.fixture
async def process(monkeypatch: Any, event_loop: Any) -> SubprocessManager:

    returncode: asyncio.Future = event_loop.create_future()
    state = SubprocessState(hang=False, returncode=returncode)

    async def proc_wait() -> int:
        log.debug("Waiting for returncode")
        if state.hang:
            while True:
                await asyncio.sleep(0.01)
        rc = await asyncio.wait_for(asyncio.shield(returncode), timeout=1)
        log.debug(f"Got returncode: {rc}")
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
    return SubprocessManager(proc, css, state)


@pytest.fixture
async def server(process: SubprocessManager, uri: URI, zctx: zmq.asyncio.Context) -> AsyncIterator[asyncio.Future]:
    async with run_server(uri.bind, zctx) as server_task:
        yield server_task

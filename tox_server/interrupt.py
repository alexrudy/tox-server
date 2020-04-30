import asyncio
import contextlib
import functools
import logging
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Iterator
from typing import Optional

log = logging.getLogger(__name__)

__all__ = ["interrupt_handler"]


@contextlib.contextmanager
def interrupt_handler(sig: int, task_factory: Callable[[], Awaitable[Any]], oneshot: bool = False) -> Iterator[None]:
    """Adds an async interrupt handler to the event loop for the context block.

    The interrupt handler should be a callable task factory, which will create
    a coroutine which should run when the signal is recieved. `task_factory` should
    not accept any arguments.

    Parameters
    ----------
    signal: int
        Signal number (see :mod:`signal` for interrupt handler)
    task_factory: callable
        Factory function to create awaitable tasks which will be run when
        the signal is recieved.

    """
    loop = asyncio.get_running_loop()

    sig_oneshot: Optional[int]
    if oneshot:
        sig_oneshot = sig
    else:
        sig_oneshot = None

    loop.add_signal_handler(
        sig, functools.partial(signal_interrupt_handler, loop=loop, task_factory=task_factory, sig=sig_oneshot)
    )

    yield

    loop.remove_signal_handler(sig)


def signal_interrupt_handler(
    loop: asyncio.AbstractEventLoop, task_factory: Callable, sig: Optional[int] = None
) -> None:
    """
    Callback used for :meth:`asyncio.AbstractEventLoop.add_signal_handler`
    to schedule a coroutine when a signal is recieved.
    """
    if sig is not None:
        loop.remove_signal_handler(sig)

    loop.create_task(task_factory())

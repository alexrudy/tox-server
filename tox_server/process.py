import asyncio
import base64
import enum
import logging
import os
import shlex
import subprocess
import sys
from typing import IO
from typing import List

import zmq.asyncio

from .protocol import Command
from .protocol import Message

log = logging.getLogger(__name__)

__all__ = ["Stream", "tox_command"]


class Stream(enum.Enum):
    """Stream switch for output"""

    STDOUT = enum.auto()
    STDERR = enum.auto()

    def _get_stream(self) -> IO[bytes]:
        """Return the stream object"""
        if self == self.STDERR:
            return getattr(sys.stderr, "buffer", sys.stderr)
        elif self == self.STDOUT:
            return getattr(sys.stdout, "buffer", sys.stdout)
        else:  # pragma: nocover
            raise ValueError(self)

    def fwrite(self, data: bytes) -> None:
        """Write and flush this stream"""
        s = self._get_stream()
        s.write(data)
        s.flush()


async def tox_command(
    args: List[str], output: zmq.asyncio.Socket, message: Message, tee: bool = True
) -> subprocess.CompletedProcess:
    """Cause a tox command to be run asynchronously.

    Also arranges to send OUTPUT messages over the provided socket.

    Parameters
    ----------
    args: list
        Arguments to tox
    output: zmq.asyncio.Socket
        Asynchronous ZMQ socket for sending output messages from the subprocess.
    message: Message
        Message used to set up replies on the output socket.
    tee: bool
        If set (default), also print output to local streams.

    Returns
    -------
    process: subprocess.CompletedProcess
        Named tuple summarizing the process.

    """
    os.environ.setdefault("PY_COLORS", "1")
    arg_str = "tox " + " ".join(shlex.quote(a) for a in args)
    log.info(f"command: {arg_str}")

    proc = await asyncio.subprocess.create_subprocess_shell(
        arg_str, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    log.debug(f"Launched proc {proc!r}")

    assert proc.stdout is not None, "Expected to get a STDOUT stream from asyncio"
    assert proc.stderr is not None, "Expected to get a STDERR stream from asyncio"

    log.debug(f"Launching output tasks")

    output_tasks = {
        asyncio.create_task(publish_output(proc.stdout, output, message, Stream.STDOUT, tee=tee)),
        asyncio.create_task(publish_output(proc.stderr, output, message, Stream.STDERR, tee=tee)),
    }

    log.debug(f"Waiting for subprocess")

    try:
        returncode = await proc.wait()
    except asyncio.CancelledError:
        log.debug(f"Cancelling subprocess")
        proc.terminate()
        returncode = await proc.wait()
    except Exception:  # pragma: nocover
        log.exception("Exception in proc.wait")
        raise
    finally:
        log.debug(f"Cleaning up")
        for task in output_tasks:
            task.cancel()

        await asyncio.wait(output_tasks)

    log.info(f"command done")

    return subprocess.CompletedProcess(args, returncode=returncode)


async def publish_output(
    reader: asyncio.StreamReader, socket: zmq.asyncio.Socket, message: Message, stream: Stream, tee: bool = False
) -> None:
    """Publish stream data to a ZMQ socket.

    Parameters
    ----------
    reader: asyncio.StreamReader
        Source reader for providing data
    socket: zmq.asyncio.Socket
        Socket for output messages
    message: Message
        Message template (provides identifiers)
    stream: Stream
        Enum identifying the stream in question (STDERR/STDOUT)
    tee: bool
        Whether to also send data to the local versions of the
        requested streams.

    """

    while True:
        data = await reader.read(n=1024)
        if data:
            message = message.respond(
                Command.OUTPUT, args={"data": base64.b85encode(data).decode("ascii"), "stream": stream.name}
            )
            await message.send(socket)
        else:
            break
        if tee:
            stream.fwrite(data)

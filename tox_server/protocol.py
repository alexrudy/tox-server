import dataclasses as dc
import enum
import json
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

import zmq.asyncio

from .exc import TSException

__all__ = ["Command", "Message", "ProtocolError", "ProtocolFailure"]


class Command(enum.Enum):
    """Intended effect of a command sent via ZMQ"""

    ERR = enum.auto()
    #: Indicates an error in command processing

    QUIT = enum.auto()
    #: Tells the server to quit

    PING = enum.auto()
    #: Asks the server for a heartbeat

    RUN = enum.auto()
    #: Run a tox command

    OUTPUT = enum.auto()
    #: Indicates tox output

    INTERRUPT = enum.auto()
    #: Interrupt the running command

    CANCEL = enum.auto()
    #: Cancel a running tox command

    HEARTBEAT = enum.auto()
    #: Issued to let a client know the server is still alive.


@dc.dataclass(frozen=True)
class Message:
    """Protocol message format"""

    command: Command
    #: Message command

    args: Any
    #: JSON-serializable arguments to this message

    identifiers: Optional[Tuple[bytes, ...]] = None
    #: ZMQ-specific socket identifiers

    timeout: Optional[float] = None
    #: Optional timeout, indicateds heartbeat period

    @classmethod
    def parse(cls, message: List[bytes]) -> "Message":
        """Parse a list of ZMQ message frames

        Ensurses that the frames correspond to this protocol.

        Raises
        ------
        ProtocolFailure: When the message frames don't contain
            enough data to provide a safe response.
        ProtocolError: An error with the message frames, with a
            reply message indicating what went wrong.

        """
        if not message:
            raise ProtocolFailure("No message parts recieved")

        *identifiers, mpart = message

        if mpart == b"" and ((not identifiers) or identifiers[-1] != b""):
            raise ProtocolError.from_message(
                message="Invalid message missing content", identifiers=tuple(identifiers) + (b"",)
            )

        if identifiers and not identifiers[-1] == b"":
            raise ProtocolFailure(f"Invalid multipart identifiers missing delimiter: {identifiers!r}")

        try:
            mdata = json.loads(mpart)
        except (TypeError, ValueError) as e:
            raise ProtocolError.from_message(message=f"JSON decode error: {e}", identifiers=tuple(identifiers))

        try:
            command, args = mdata["command"], mdata["args"]
        except KeyError as e:
            raise ProtocolError.from_message(message=f"JSON missing key: {e}", identifiers=tuple(identifiers))

        timeout = mdata.get("timeout", None)
        if not isinstance(timeout, (float, type(None))):
            timeout = None

        try:
            command = Command[command]
        except KeyError:
            raise ProtocolError.from_message(message=f"Unknown command: {command}", identifiers=tuple(identifiers))

        return cls(command=command, args=args, identifiers=tuple(identifiers), timeout=timeout)

    def assemble(self) -> List[bytes]:
        """Assemble this message for sending"""
        message = json.dumps({"command": self.command.name, "args": self.args, "timeout": self.timeout}).encode("utf-8")
        if self.identifiers:
            return list(self.identifiers) + [message]
        return [message]

    def respond(self, command: Command, args: Any) -> "Message":
        """Create a response message object, which retains message identifiers"""
        return dc.replace(self, command=command, args=args)

    def interrupt(self) -> "Message":
        return dc.replace(self, command=Command.INTERRUPT, args={"command": self.command.name})

    @property
    def identifier(self) -> Tuple[bytes, ...]:
        """An immutable set of message identifiers"""
        if not self.identifiers:
            return (b"",)
        return tuple(self.identifiers)

    @classmethod
    async def recv(cls, socket: zmq.asyncio.Socket, flags: int = 0) -> "Message":
        """Recieve a message on the provided socket"""
        data = await socket.recv_multipart(flags=flags)
        return cls.parse(data)

    async def send(self, socket: zmq.asyncio.Socket, flags: int = 0) -> None:
        """Send a message on the provided socket"""
        await socket.send_multipart(self.assemble(), flags=flags)

    def for_dealer(self) -> "Message":
        return dc.replace(self, identifiers=tuple([b""] + list(self.identifiers or [])))


@dc.dataclass
class ProtocolError(TSException):
    """Error raised for a recoverable protocol problem.

    The :attr:`message` provides an error message to send back to the client.
    """

    message: Message

    def __str__(self) -> str:
        return self.message.args["message"]

    @classmethod
    def from_message(cls, message: str, identifiers: Optional[Tuple[bytes, ...]] = None) -> "ProtocolError":
        """Build a protocol error from a string message"""
        return cls(message=Message(Command.ERR, {"message": message}, identifiers=identifiers))


@dc.dataclass
class ProtocolFailure(TSException):
    """A protocol error we can't recover from, the message was so malformed, no response is possible"""

    message: str

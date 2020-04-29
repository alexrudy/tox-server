import json

import pytest

from tox_server.protocol import Message
from tox_server.protocol import ProtocolError
from tox_server.protocol import ProtocolFailure


class TestMessage:
    def test_parse_empty(self) -> None:

        with pytest.raises(ProtocolFailure):
            Message.parse([])

    def test_parse_invalid_identifiers(self) -> None:

        with pytest.raises(ProtocolFailure):
            Message.parse([b"identifier", b"data"])

    def test_parse_invalid_json(self) -> None:
        with pytest.raises(ProtocolError):
            Message.parse([b"identifier", b"", b"{data"])

    def test_parse_missing_args(self) -> None:

        data = json.dumps({"command": "BAZ"}).encode("utf-8")
        with pytest.raises(ProtocolError) as exc_info:
            Message.parse([b"identifier", b"", data])
        assert exc_info.value.message.args["message"] == "JSON missing key: 'args'"
        assert str(exc_info.value) == "JSON missing key: 'args'"

    def test_parse_invalid_command(self) -> None:
        data = json.dumps({"command": "BAZ", "args": None}).encode("utf-8")
        with pytest.raises(ProtocolError) as exc_info:
            Message.parse([b"identifier", b"", data])
        assert exc_info.value.message.args["message"] == "Unknown command: BAZ"
        assert str(exc_info.value) == "Unknown command: BAZ"

    def test_identifier_default(self) -> None:
        data = json.dumps({"command": "PING", "args": None}).encode("utf-8")
        msg = Message.parse([data])
        assert not msg.identifiers
        assert msg.identifier == (b"",)

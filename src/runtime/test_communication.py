# File: tests/test_communication.py

from enum import Enum
from unittest.mock import MagicMock, patch, Mock

import pytest
from pydantic import BaseModel
from src.runtime.communication import (
    CommunicationBackend,
    Message,
    CommandName,
    TerminalCommunicationBackend,
)


def test_model_is_accepted_as_message_data(self):
    class TestModel(BaseModel):
        example: str

    message = Message(command=CommandName.ACK, data=TestModel(example="value"))
    assert message.data.example == "value"
    assert message.model_dump_json() == '{"command":"ACK","data":{"example":"value"}}'


class TestCommunicationBackend:

    class ConcreteCommunicationBackend(CommunicationBackend):
        def _send_message(self, message: Message) -> Message:
            return message

    @pytest.fixture(scope="class")
    def backend(self):
        return self.ConcreteCommunicationBackend()

    def test_get_task_stop_signal(self, backend):
        stop_message = Message(command=CommandName.STOP)
        backend._send_message = Mock(return_value=stop_message)

        assert backend.get_task() is None

    def test_get_task_returns_task_info(self, backend):
        task_data = {"task_id": 1, "task_name": "test_task"}
        response_message = Message(command=CommandName.TASK_INFO, data=task_data)
        backend._send_message = Mock(return_value=response_message)

        result = backend.get_task()
        assert result == task_data

    def test_create_datum_returns_datum_info(self, backend):
        datum_data = {"datum_id": 1}
        response_message = Message(command=CommandName.DATUM_INFO, data=datum_data)
        backend._send_message = Mock(return_value=response_message)

        result = backend.create_datum()
        assert result == datum_data

    def test_commit_datum_successful_ack(self, backend):
        response_message = Message(command=CommandName.ACK)
        backend._send_message = Mock(return_value=response_message)

        # No exception should be raised
        backend.commit_datum({"datum_id": 1})

    def test_notify_task_completion_successful_ack(self, backend):
        response_message = Message(command=CommandName.ACK)
        backend._send_message = Mock(return_value=response_message)

        # No exception should be raised
        backend.notify_task_completion({"task_id": 1})


class TestTerminalCommunicationBackend:

    @pytest.fixture(scope="class")
    def backend(self):
        return TerminalCommunicationBackend()

    def test_send_message_outputs_to_stdout(self, backend, capsys):
        test_message = Message(command=CommandName.GET_TASK, data={"key": "value"})
        with patch.object(backend, "get_response", return_value=test_message):
            backend._send_message(test_message)
            captured = capsys.readouterr()
            assert TerminalCommunicationBackend.OUT_SEPARATOR in captured.out
            assert '"key":"value"' in captured.out

    def test_get_response_parses_valid_input(self, backend):
        input_line = f'{TerminalCommunicationBackend.IN_SEPARATOR}{{"command": "ACK", "data": {{"example": "value"}}}}'
        with patch("sys.stdin", [input_line]):
            response = backend.get_response()
            assert response.command == CommandName.ACK
            assert response.data["example"] == "value"

    def test_get_response_ignored_invalid_input(self, backend):
        input_line = "Invalid input line"
        with patch("sys.stdin", [input_line]):
            response = backend.get_response()
            assert response is None

    def test_parse_line_with_valid_separator(self, backend):
        line = f'{TerminalCommunicationBackend.IN_SEPARATOR}{{"command":"STOP"}}'
        parsed_message = backend._parse_line(line)
        assert parsed_message.command == CommandName.STOP

    def test_parse_line_returns_none_for_invalid_line(self, backend):
        line = "Some irrelevant log line"
        parsed_message = backend._parse_line(line)
        assert parsed_message is None

    def test_parse_line_detects_invalid_json(self, backend):
        line = f"{TerminalCommunicationBackend.IN_SEPARATOR}Invalid JSON"
        with pytest.raises(ValueError):
            parsed_message = backend._parse_line(line)

    def test_parse_line_detects_ignore_non_command(self, backend):
        line = f"Invalid JSON command"
        parsed_message = backend._parse_line(line)
        assert parsed_message is None

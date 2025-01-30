from unittest.mock import patch, Mock

import pytest
from pydantic import BaseModel
from src.runtime.communication import (
    CommunicationBackend,
    Message,
    CommandName,
    TerminalCommunicationBackend,
    SimpleCliCommunicationBackend,
)


def test_model_is_accepted_as_message_data():
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

        assert backend.get_job() is None

    def test_create_datum_returns_datum_info(self, backend):
        datum_data = {"datum_id": 1}
        response_message = Message(
            command=CommandName.DATUM_DEFINITION, data=datum_data
        )
        backend._send_message = Mock(return_value=response_message)

        result = backend.create_datum()
        assert result == datum_data

    def test_commit_datum_successful_ack(self, backend):
        response_message = Message(command=CommandName.ACK)
        backend._send_message = Mock(return_value=response_message)

        # No exception should be raised
        backend.commit_datum({"datum_id": 1})


class TestTerminalCommunicationBackend:

    @pytest.fixture(scope="class")
    def backend(self):
        return TerminalCommunicationBackend()

    def test_send_message_outputs_to_stdout(self, backend, capsys):
        test_message = Message(command=CommandName.GET_JOB, data={"key": "value"})
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


class TestSimpleCliBackend:
    """This tests the SimpleCliCommunicationBackend class"""

    @pytest.fixture()
    def backend(self):
        return SimpleCliCommunicationBackend()

    def test_parse_args_with_component_and_no_file(self):
        args = SimpleCliCommunicationBackend._parse_args(["component_name"])
        assert args.component == "component_name"
        assert args.file is None
        assert args.output is None

    def test_parse_args_raises_error_with_both_file_and_component(self):
        with pytest.raises(
            SystemExit
        ):  # argparse raises SystemExit by default on error
            SimpleCliCommunicationBackend._parse_args(
                ["component_name", "--file", "job.json"]
            )

    def test_parse_args_accepts_valid_kv_argument(self):
        args = SimpleCliCommunicationBackend._parse_args(
            ["_", "-a", "key1=value1,key2=value2"]
        )
        assert args.argument == {"key1": "value1", "key2": "value2"}

    def test_parse_args_rejects_invalid_kv_pairs(self):
        with pytest.raises(SystemExit):  # argparse raises SystemExit on invalid input
            SimpleCliCommunicationBackend._parse_args(["--argument", "invalid_pair"])

    def test_parse_args_requires_component_or_file(self):
        with pytest.raises(
            SystemExit
        ):  # argparse raises SystemExit by default on error
            SimpleCliCommunicationBackend._parse_args([])

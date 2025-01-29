import abc
import sys
from enum import StrEnum
from typing import override


from pydantic import BaseModel, Field, SerializeAsAny

from runtime.component_definition import JobDefinition


# There is a lot of redundancy having to declare a new command on the enum,
# class mapping, pydantic model and the union type.
# On the other hand it makes the code easier to handle from the outside.


class CommandName(StrEnum):
    """Represents a message sent TO or FROM the cubelet."""

    # From runtime to cubelet
    GET_JOB = "GET_JOB"
    JOB_FINISHED = "JOB_FINISHED"
    CREATE_DATUM = "CREATE_DATUM"
    COMMIT_DATUM = "COMMIT_DATUM"

    # From cubelet to runtime
    JOB_DEFINITION = "JOB_DEFINITION"
    DATUM_DEFINITION = "DATUM_DEFINITION"
    ACK = "ACK"
    STOP = "STOP"
    ERROR = "ERROR"


class Message(BaseModel):
    command: CommandName
    data: dict | SerializeAsAny[BaseModel] = Field(  # noqa: intellij bug
        default_factory=dict
    )


class CommunicationBackend(abc.ABC):
    """Handles communication with the cubelet.

    The communication with the cubelet is done through a request-response pattern.
    """

    @abc.abstractmethod
    def _send_message(self, message: Message) -> Message:
        """Sends a message to the cubelet and returns the response."""
        pass

    def get_job(self) -> JobDefinition | None:
        """Request a new job from the cubelet. It might respond with a job or a shutdown signal.

        The None return value should be interpreted as a shutdown signal.
        """

        response = self._send_message(Message(command=CommandName.GET_JOB))
        if response.command == CommandName.STOP:
            return None
        elif response.command == CommandName.JOB_DEFINITION:
            job = JobDefinition.model_validate(response.data)
            return job

        raise ValueError(f"Unexpected response from get job: {response}")

    def create_datum(self):
        """Request the creation of a new empty datum."""
        response = self._send_message(Message(command=CommandName.CREATE_DATUM))

        if response.command == CommandName.DATUM_DEFINITION:
            return response.data

        raise ValueError(f"Unsuccessful datum request: {response}")

    def commit_datum(self, datum):
        """Commit the changes made to a new datum."""
        response = self._send_message(
            Message(command=CommandName.COMMIT_DATUM, data=datum)
        )
        if response.command != CommandName.ACK:
            raise ValueError(f"Unsuccessful datum commit: {response}")

    def notify_job_completion(self, job: JobDefinition):
        """Notify the Cubelet that a job has finished."""
        response = self._send_message(
            Message(command=CommandName.JOB_FINISHED, data=job)
        )
        if response.command != CommandName.ACK:
            raise ValueError(f"Unsuccessful job notification: {response}")


class TerminalCommunicationBackend(CommunicationBackend):
    """
    Handles communication with the cubelet thought process StdOut and StdIn.

    It's the simplest communication backend. It has the drawback of being intertwined with normal logs.
    """

    # Prefix to differentiate protocol messages from normal log
    OUT_SEPARATOR = "<--RUNTIME-->"
    IN_SEPARATOR = "<--CUBELET-->"

    @override
    def _send_message(self, message: Message):
        # If this fails will make the runtime fail as well
        payload = message.model_dump_json()
        print(f"{self.OUT_SEPARATOR}{payload}")
        return self.get_response()

    def get_response(self) -> Message:
        """Waits for a response from the cubelet.

        If no response is received, we get stuck.
        The cubelet will have the responsibility of shutting us down.
        """
        for line in sys.stdin:
            command = self._parse_line(line)
            if command is not None:
                return command

    def _parse_line(self, line: str) -> Message | None:
        """Tries to parse a line.

        If the line could not be parsed because it did not contain a Message object, nothing is returned
        """
        if not line.startswith(self.IN_SEPARATOR):
            return None

        potential_json_literal = line[len(self.IN_SEPARATOR) :].strip()
        return Message.model_validate_json(potential_json_literal)

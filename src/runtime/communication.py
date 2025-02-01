import abc
import sys
from enum import StrEnum
from pathlib import Path
from typing import override
import argparse

from pydantic import BaseModel, Field, SerializeAsAny

from runtime.command_line import kv_pairs, parse_kv_pairs
from runtime.component_definition import JobDefinition, TaskDefinition
from runtime.persistance import UnspecifiedDatum, DatumDefinition


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


class SimpleCliCommunicationBackend(CommunicationBackend):
    """Gets all the data from the command line arguments.

    It is intended to run a single task. If the runtime tries to do more it will fail.
    """

    def __init__(self):
        self.args = self._parse_args(sys.argv[1:])
        self.job = self._parse_job()
        self.job_completed = False

    def _send_message(self, message: Message) -> Message:
        """Simulate responses based on the job definition.

        Fail if the context tries to do more than what is allowed.
        """
        match message.command:
            case CommandName.GET_JOB:
                if self.job is not None:
                    return Message(command=CommandName.JOB_DEFINITION, data=self.job)
                else:
                    return Message(command=CommandName.STOP)
            case CommandName.JOB_FINISHED:
                if self.job_completed:
                    return Message(
                        command=CommandName.ERROR,
                        data={
                            "msg": "Job already completed. This communication backend only supports a single job."
                        },
                    )
                else:
                    self.job_completed = True
                    return Message(command=CommandName.ACK)
            case CommandName.CREATE_DATUM:
                return Message(
                    command=CommandName.ERROR,
                    data={
                        "msg": f"The {self.__class__.__name__} does not support creating datums. Use the --input option to specify a path to an existing datum or the --output option to create a new one."
                    },
                )
            case CommandName.COMMIT_DATUM:
                # The data should be where the user requested, it's their responsibility to actually commit it.
                return Message(command=CommandName.ACK)
            case _:
                return Message(
                    command=CommandName.ERROR,
                    data={
                        "msg": f"Command unknown or not supported by {self.__class__.__name__}"
                    },
                )

    @staticmethod
    def _parse_args(
        raw_args: list[str],
    ):
        """

        Args are passed explicitly for testability
        """
        parser = argparse.ArgumentParser(
            description="Simple Command-line Communication Backend"
        )

        parser.add_argument(
            "component", type=str, nargs="?", help="ID of the component you want to run"
        )

        parser.add_argument(
            "-f",
            "--file",
            type=str,
            help="File containing the job definition.",
            required=False,
        )
        parser.add_argument("-o", "--output", type=str, help="Optional output path")
        parser.add_argument("-i", "--input", type=str, help="Optional input datum path")

        parser.add_argument(
            "-a",
            "--argument",
            type=kv_pairs,
            help="Optional option to pass to the component.. Expects comma-separated key=value pairs.",
            required=False,
            action="append",
        )

        parser.add_argument(
            "-c",
            "--context",
            type=kv_pairs,
            help="Optional context options. Expects comma-separated key=value pairs.",
            required=False,
            action="append",
        )

        args = parser.parse_args(raw_args)
        args.argument = parse_kv_pairs(args.argument)
        args.context = parse_kv_pairs(args.context)

        if args.file is None and args.component is None:
            parser.error("Either --file or component must be specified")
        elif args.file is not None and args.component is not None:
            parser.error("Only one of --file or component can be specified")

        return args

    @staticmethod
    def _add_datum(datums: list, path: Path):
        if path is not None:
            datum = UnspecifiedDatum(DatumDefinition(path=path))
            datums.append(datum)
        return datums

    def _parse_job(self):
        if self.args.file is not None:
            return JobDefinition.model_validate_file(self.args.file)

        # Reconstruct task from args
        task = TaskDefinition(
            name=self.args.component,
            library="local",
            arguments=self.args.argument,
            input_data=self._add_datum([], self.args.input),
            output_data=self._add_datum([], self.args.output),
        )

        return JobDefinition(tasks=[task])


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

    def get_response(
        self,
    ) -> Message:  # noqa: This method does indeed only return a Message
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

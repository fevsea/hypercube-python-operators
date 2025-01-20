import abc
import logging

import typing
import asyncio

from pydantic import BaseModel, Field

from runtime.communication import CommunicationBackend


class DatumDefinition(BaseModel):
    """Describes a datum that can be used in an Operator."""

    class Config:
        extra = "allow"


class TaskDefinition(BaseModel):
    """Describes a task that can be executed by the runtime."""

    library: str
    operator: str
    version: str

    options: dict = Field(default_factory=dict)  # noqa: intellij bug
    input_data: list[DatumDefinition] = Field(
        default_factory=list
    )  # noqa: intellij bug
    output_data: list[DatumDefinition] = Field(
        default_factory=list
    )  # noqa: intellij bug

    class Config:
        extra = "allow"


class Runtime:
    """Handles the execution of tasks assigned by the cubelet."""

    def __init__(self, communication_backend: CommunicationBackend):
        self.communication_backend: CommunicationBackend = communication_backend
        self.logger = logging.getLogger("Runtime")

    def start(self):
        """Starts the task execution loop."""
        while True:
            next_task = self.communication_backend.get_task()
            if next_task is None:
                self.logger.info("Shutdown signal received.")
                return

            self.run_task(task=next_task)

    def run_task(self, task):
        pass


class Context:
    """Object passed to Operators that allows them to interact with the runtime.

    It is mainly a facade class for the runtime that simplifies the interface while ensuring users don't mess
    with the runtime directly.

    This API should be very stable, so introducing a layer of indirection will simplify changes.
    """

    def __init__(self, runtime: Runtime):
        self._runtime: Runtime = runtime

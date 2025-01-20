import abc
import logging

import typing
import asyncio

from pydantic import BaseModel, Field

from runtime.catalog_base import Catalog
from runtime.communication import CommunicationBackend
from runtime.operator_definition import JobDefinition


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
    input_data: list[DatumDefinition] = Field(  # noqa: intellij bug
        default_factory=list
    )
    output_data: list[DatumDefinition] = Field(  # noqa: intellij bug
        default_factory=list
    )

    class Config:
        extra = "allow"


class Runtime:
    """Handles the execution of jobs assigned by the cubelet."""

    def __init__(self, catalog: Catalog, communication_backend: CommunicationBackend):
        self.catalog: Catalog = catalog
        self.communication_backend: CommunicationBackend = communication_backend
        self.logger = logging.getLogger("Runtime")

    def start(self):
        """Starts the job execution loop."""
        while True:
            next_job = self.communication_backend.get_job()
            if next_job is None:
                self.logger.info("No jobs left. Shutting down runtime.")
                return

            self.run_job(job=next_job)

    def run_job(self, job: JobDefinition):
        pass


class Context:
    """Object passed to Operators that allows them to interact with the runtime.

    It is mainly a facade class for the runtime that simplifies the interface while ensuring users don't mess
    with the runtime directly.

    This API should be very stable, so introducing a layer of indirection will simplify changes.
    """

    def __init__(self, runtime: Runtime):
        self._runtime: Runtime = runtime

import abc
import logging

import typing
import asyncio
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field

from runtime.catalog_base import Catalog
from runtime.communication import CommunicationBackend
from runtime.operator_definition import JobDefinition, Operator
from runtime.persistance import NilDatum, DatumDefinition, FileDatum, FolderDatum, Datum


class TaskDefinition(BaseModel):
    """Describes a task that can be executed by the runtime."""

    class Config:
        extra = "allow"

    library: str
    operator: str
    version: str

    options: dict = Field(default_factory=dict)  # noqa: intellij bug
    input_data: list[DatumDefinition | None | list[DatumDefinition]] = Field(  # noqa: intellij bug
        default_factory=list
    )


class Runtime:
    """Handles the execution of jobs assigned by the cubelet."""

    def __init__(self, catalog: Catalog, communication_backend: CommunicationBackend):
        self.catalog: Catalog = catalog
        self.communication_backend: CommunicationBackend = communication_backend
        self.logger = logging.getLogger("Runtime")
        self.datum_cache: dict[str, Datum] = dict()

    def start(self):
        """Starts the job execution loop."""
        while True:
            next_job = self.communication_backend.get_job()
            if next_job is None:
                self.logger.info("No jobs left. Shutting down runtime.")
                return

            self.run_job(job=next_job)

    def run_job(self, job: JobDefinition):
        self.logger.info(f"Running job: {job}")
        for task in job.tasks:
            self.run_task(task)

    def run_task(self, task: TaskDefinition):
        operation = self._build_operation(task)
        result = operation.run()
        self.communication_backend.commit_datum(result)

    def _get_datum(
        self, datum_definition: DatumDefinition | None | list[DatumDefinition]
    ):
        """Returns an adequate datum instance for the given definition.

        It will make sure the data is ready to be consumed.
        """
        if datum_definition is None:
            return None

        if type(datum_definition) is list:
            return [self._get_datum(datum) for datum in datum_definition]

        if not datum_definition.hash in self.datum_cache:
            self.datum_cache[datum_definition.hash] = Datum.datum_factory(datum_definition)

        return self.datum_cache[datum_definition.hash]

    def _build_operation(self, task: TaskDefinition) -> Operator:
        """Converts a task definition into an executable operator."""
        operator_class = self.catalog.get_operator_for_task(task)
        options = operator_class.Options.model_validate(task.options)
        input_data = [self._get_datum(datum_definition) for datum_definition in task.input_data]

        return operator_class(input_data=input_data, options=options)


class Context:
    """Object passed to Operators that allows them to interact with the runtime.

    It is mainly a facade class for the runtime that simplifies the interface while ensuring users don't mess
    with the runtime directly.

    This API should be very stable, so introducing a layer of indirection will simplify changes.
    """

    def __init__(self, runtime: Runtime):
        self._runtime: Runtime = runtime

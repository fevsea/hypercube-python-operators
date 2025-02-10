import logging
from functools import partial

from pydantic import BaseModel, Field

from runtime.catalog_base import Catalog
from runtime.communication import CommunicationBackend
from runtime.component_definition import JobDefinition, TaskDefinition
from runtime.context import Context
from runtime.persistance import DatumDefinition, Datum, DatumFactory


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
        component = self.catalog.get_component(task.component, task.library)
        input_data = self._get_datums(task.input_data)
        output_data = self._get_datums(task.output_data)
        options = task.options
        context = self._build_context_for_task(task)
        result = component.run(
            context=context,
            input_data=input_data,
            output_data=output_data,
            options=options,
        )
        self.logger.info(f"Task {task.component} finished with result: {result}")
        self.commit_datums(output_data.values())

    def _get_datum(
        self, datum_definition: DatumDefinition | None | list[DatumDefinition]
    ) -> None | Datum | list[Datum]:
        """Returns an adequate datum instance for the given definition.

        It will make sure the data is ready to be consumed.
        """
        if datum_definition is None:
            return None

        if type(datum_definition) is list:
            return [self._get_datum(datum) for datum in datum_definition]

        if datum_definition.hash is None:
            return Datum.datum_factory(datum_definition)

        if not datum_definition.hash in self.datum_cache:
            self.datum_cache[datum_definition.hash] = Datum.datum_factory(
                datum_definition
            )

        return self.datum_cache[datum_definition.hash]

    def _get_datums(
        self, datums_definitions: dict[str, DatumDefinition]
    ) -> dict[str, Datum | list[Datum]]:
        """Converts dict of DatumDefinitions into datums."""
        return {
            name: self._get_datum(datum_definition)
            for name, datum_definition in datums_definitions.items()
        }

    def _build_context_for_task(self, task: TaskDefinition):
        return Context(self, task)

    def commit_datums(self, datums: list[Datum]):
        for datum in datums:
            if isinstance(datum, DatumFactory):
                self.commit_datums(datum.generated_datums)
            else:
                self.communication_backend.commit_datum(datum.get_definition())

import logging

from pydantic import BaseModel, Field

from runtime.catalog_base import Catalog
from runtime.communication import CommunicationBackend
from runtime.component_definition import JobDefinition, Component
from runtime.context import Context
from runtime.persistance import DatumDefinition, Datum


class TaskDefinition(BaseModel):
    """Describes a task that can be executed by the runtime."""

    class Config:
        extra = "allow"

    library: str
    component: str
    version: str

    options: dict = Field(default_factory=dict)  # noqa: intellij bug
    input_data: list[DatumDefinition | None | list[DatumDefinition]] = Field(
        default_factory=list
    )  # noqa: intellij bug


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
        component = self._build_component(task)
        result = component.run()
        self.communication_backend.commit_datum(result)

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

        if not datum_definition.hash in self.datum_cache:
            self.datum_cache[datum_definition.hash] = Datum.datum_factory(
                datum_definition
            )

        return self.datum_cache[datum_definition.hash]

    def _build_component(self, task: TaskDefinition) -> Component:
        """Converts a task definition into an executable component."""
        component_class = self.catalog.get_component_for_task(task)
        options = component_class.Options.model_validate(task.options)
        input_data = tuple(
            self._get_datum(datum_definition) for datum_definition in task.input_data
        )
        context = self._build_context_for_class(task)

        return component_class(context=context, input_data=input_data, options=options)

    def _build_context_for_class(self, task: TaskDefinition):
        return Context(self, task)

import abc
import re
from enum import StrEnum
from typing import Any, Callable, Type

from pydantic import BaseModel, Field

from runtime.persistance import Datum, DatumDefinition
from runtime.context import Context


class TaskDefinition(BaseModel):
    """Defines a single executable operation.

    It only holds the definition, it cannot be executed by itself.
    """

    # Component info
    name: str
    library: str = "local"

    # Parameters
    options: dict[str, Any] = Field(default_factory=list)
    input_data: list[DatumDefinition] = Field(default_factory=list)
    output_data: list[DatumDefinition] = Field(default_factory=list)


class JobDefinition(BaseModel):
    tasks: list[TaskDefinition]


class ComponentTags(StrEnum):
    """Enum with some common tags.

    It's not an exhaustive list, as tags are just arbitrary strings.
    An enum is a convenient way to suggest common tags to devs.
    """

    # Role
    IMPORTER = "importer"
    EXPORTER = "exporter"
    TRANSFORMER = "transformer"
    ANALYZER = "analyzer"
    VISUALIZER = "visualizer"
    MODEL = "model"
    SIMULATOR = "simulator"
    EVALUATOR = "evaluator"

    # Formats
    RAW = "raw"
    TIMESERIES = "timeseries"


class IoType(StrEnum):
    FILE = "file"
    FOLDER = "folder"
    DATAFRAME = "dataframe"
    OBJECT = "object"


class SlotDefinition(BaseModel):
    """Describes an input/output slot.

    Two components can be connected if they have compatible IoSlot objects.
    """

    # Meta
    name: str
    labels: set[str] = set()

    # What kind of data is expected
    required: bool = Field(default=True)
    multiple: bool = Field(default=False)
    type: IoType = Field(default=IoType.FOLDER)


class Component(abc.ABC):
    """Define the interface for a component."""

    # Metadata about the component
    name: str = "GenericComponent"
    description: str = ""
    labels: set[str] = set()

    # Describes the specific I/O shape of the Component.
    input_slots: tuple[SlotDefinition, ...] = tuple()
    output_slots: tuple[SlotDefinition, ...] = tuple()

    class Options(BaseModel):
        """A component can have arbitrary options.

        This is ideally a single-level object. By using a Pydantic model, we can define its range.
        """
        class Config:
            extra = "allow"

    def __init__(
        self,
        context: Context,
        input_data: tuple[Datum | None | list[Datum], ...],
        output_data: tuple[Datum | None | list[Datum], ...],
        options: Options,
    ):
        # An instance is tied to actual data and parameters
        self.input_data = self._validate_slots(input_data, self.input_slots)
        self.output_data = self._validate_slots(output_data, self.output_slots)
        self.options = options
        self.context = context
        self.logger = context.get_logger(self.name)

    @staticmethod
    def _validate_slots(
        datums: tuple[Datum, ...],
        slots: tuple[SlotDefinition, ...],
    ):
        if len(slots) != len(datums):
            # The zip validation below requires this invariant
            raise ValueError(
                f"The number of input slots ({len(datums)}) does not match the number of required input slots ({len(slots)})"
            )

        for slot, datum in zip(slots, datums):
            if slot.required and datum is None:
                raise ValueError(f"Slot {slot.name} is required but not provided.")
            if slot.type != datum.get_type():
                raise ValueError(
                    f"Slot {slot.name} is of type {slot.type} but the provided datum is of type {datum.get_type()}"
                )
        # The content of list datum is not validated
        return datums

    def run(self) -> tuple[Datum]:
        pass

    def get_human_name(self):
        """Return a human-readable name for the component."""
        return re.sub(r"\W+", " ", self.name)

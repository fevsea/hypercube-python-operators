import abc
import re
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from runtime.persistance import Datum
from runtime.context import Context


class TaskDefinition(BaseModel):
    """Defines a single executable operation.

    It only holds the definition, it cannot be executed by itself.
    """

    # Component info
    name: str
    library: str = "local"

    # Parameters
    arguments: dict[str, Any] = Field(default_factory=list)
    input_data: list[Datum] = Field(default_factory=list)
    output_data: list[Datum] = Field(default_factory=list)


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
    meta_labels: set[str] = set()

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

        pass

    def __init__(
        self,
        context: Context,
        input_data: tuple[Datum | None | list[Datum], ...],
        options: Options,
    ):
        # An instance is tied to actual data and parameters
        self.input_data = input_data
        self.options = options
        self.context = context
        self.logger = context.get_logger(self.name)

        # Check we have all required fields
        if len(self.input_slots) != len(input_data):
            raise ValueError(
                f"The number of input slots ({len(input_data)}) does not match the number of required input slots ({len(self.input_slots)})"
            )
        for slot, data in zip(self.input_slots, input_data):
            if slot.required and data is None:
                raise ValueError(f"Slot {slot.name} is required but not provided.")

    def run(self) -> tuple[Datum]:
        pass

    def get_human_name(self):
        """Return a human-readable name for the component."""
        return re.sub(r"\W+", " ", self.name)

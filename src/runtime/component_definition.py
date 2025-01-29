import abc
import logging
from enum import StrEnum
import re
from pathlib import Path
from typing import Any

from pandas import DataFrame
from pydantic import BaseModel, Field

from runtime.persistance import Datum


class TaskDefinition(BaseModel):
    """Defines a single executable operation.

    It only holds the definition, it cannot be executed by itself.
    """

    # Component info
    name: str
    library: str
    arguments: dict[str, Any]


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

    name: str
    tags: set[str] = tuple()

    required: bool = Field(default=True)
    # Whether the slot expects a list or a single instance of the declared type
    multiple: bool = Field(default=False)
    # What is the underlying data format expected.
    type: IoType = Field(default=IoType.FOLDER)


class Component(abc.ABC):
    """Define the interface for a component."""

    # Metadata about the component
    meta_name: str = "GenericComponent"
    meta_description: str = ""
    meta_labels: set[str] = set()

    # Describes the specific I/O shape of the Component.
    meta_input_slots: tuple[SlotDefinition, ...] = tuple()
    meta_output_slots: tuple[SlotDefinition, ...] = tuple()

    class Options(BaseModel):
        """A component can have arbitrary options.

        This is ideally a single-level object. By using a Pydantic model, we can define its range.
        """

        pass

    def __init__(self, input_data: tuple[Datum | None | list[Datum]], options: Options):
        # Anz instance is tied to actual data and parameters
        self.input_data = input_data
        self.options = options
        self.logger = logging.getLogger(self.meta_name)

        # Check we have all required fields
        if len(self.meta_input_slots) != len(input_data):
            raise ValueError(
                f"The number of input slots ({len(input_data)}) does not match the number of required input slots ({len(self.meta_input_slots)})"
            )
        for slot, data in zip(self.meta_input_slots, input_data):
            if slot.required and data is None:
                raise ValueError(f"Slot {slot.name} is required but not provided.")

    def run(self) -> tuple[Datum]:
        pass

    def get_human_name(self):
        """Return a human-readable name for the component."""
        return re.sub(r"\W+", " ", self.meta_name)

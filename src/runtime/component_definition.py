import abc
from dataclasses import dataclass
import functools
import inspect
import re
from datetime import datetime
from enum import StrEnum
from typing import Any, Callable, Type, TypedDict, TypeVar

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


@dataclass
class SlotDefinition:
    """Describes an input/output slot.

    Two components can be connected if they have compatible IoSlot objects.
    """

    # Meta
    name: str
    description: str = ""

    # What kind of data is expected
    required: bool = Field(default=True)
    multiple: bool = Field(default=False)
    type: IoType = Field(default=IoType.FOLDER)


@dataclass
class OptionDefinition:
    """Describes an option parameter."""

    class Types(StrEnum):
        """Types of options."""

        STRING = "string"
        INTEGER = "integer"
        FLOAT = "float"
        BOOLEAN = "boolean"
        DATETIME = "datetime"

        @classmethod
        def from_type(cls, python_type: Type):
            if python_type == str:
                return cls.STRING
            elif python_type == int:
                return cls.INTEGER
            elif python_type == float:
                return cls.FLOAT
            elif python_type == bool:
                return cls.BOOLEAN
            elif python_type == datetime:
                return cls.DATETIME
            else:
                raise ValueError(f"Unknown type: {python_type}")

    name: str
    description: str = ""
    type: Types = Types.STRING
    default: any = None
    required: bool = True
    min: None | int | float = None
    max: None | int | float = None
    enum: None | list[str] = None


type OptionTypes = str | int | float | bool


class Component:
    """Holds a runnable component."""

    def __init__(
        self,
        runnable: Callable,
        name: str,
        input_slots: dict[str, SlotDefinition] = None,
        output_slots: dict[str, SlotDefinition] = None,
        options: dict[str, OptionDefinition] = None,
        description: str = "",
        labels: set[str] = None,
        context_varname: bool = None,
        version: str = "1",
    ):
        # Metadata about the component
        self.name: str = name
        self.description: str = description
        self.labels: set[str] = labels or set()

        # Describes the specific I/O shape of the Component.
        self.input_slots: dict[str, SlotDefinition] = input_slots or {}
        self.output_slots: dict[str, SlotDefinition] = output_slots or {}
        self.available_options: dict[str, OptionTypes] = options or {}

        # Calling ingo
        self.runnable: Callable = runnable
        self.context_varname: str = context_varname

    def run(
        self,
        context: Context,
        input_data: dict[Datum | None | list[Datum], ...],
        output_data: dict[Datum | None | list[Datum], ...],
        options: dict[str, OptionTypes] = None,
    ):
        """Actually executes the component with specific data."""
        # An instance is tied to actual data and parameters
        # Todo: Rewrite validation
        input_data = self._validate_slots(input_data, self.input_slots)
        output_data = self._validate_slots(output_data, self.output_slots)
        options = options or dict()
        context = context

        params = {**options, **input_data, **output_data}
        if self.context_varname is not None:
            params[self.context_varname] = context

        self.runnable(**params)

    @staticmethod
    def _validate_slots(
        datums: dict[str, Datum],
        slots: dict[str, SlotDefinition],
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


###
# Decorator
###


@dataclass
class InoutType:
    """Type used with the command_c component decorator to denote the type of parameter."""

    description: str = ""
    name: str = None
    required: bool = Field(default=True)
    multiple: bool = Field(default=False)
    type: IoType = Field(default=IoType.FOLDER)


class Input(InoutType):
    """Represents an input parameter for a component function."""


class Output(InoutType):
    """Represents an output parameter for a component function."""


@dataclass
class Option:
    """Represents an option parameter for a component function."""

    type: Type = str
    description: str = ""
    default: any = None
    required: bool = True
    min: None | int | float = None
    max: None | int | float = None
    enum: None | list[str] = None


def command_component(
    name: str,
    version: str = "1",
    description: str = "",
    labels: set[str] = None,
) -> Callable:
    """Decorator that defines a runnable component.

    It runs
    """

    def decorator(func: Callable) -> Callable:
        if not callable(func) or not inspect.isfunction(func):
            raise TypeError(
                "The @command_component decorator can only be applied to functions."
            )

        # Extract the signature and annotations of the function
        signature = inspect.signature(func)
        annotations = func.__annotations__

        # Metadata for the component
        metadata = {
            "name": name,
            "version": version,
            "labels": labels,
            "description": description,
            "input_slots": {},
            "output_slots": {},
            "options": {},
            "context_varname": None,
        }

        # Parse the function arguments using its annotations
        for param_name, param in signature.parameters.items():
            if param_name in annotations:
                annotation = annotations[param_name]
                if isinstance(annotation, Option):
                    metadata["options"][param_name] = OptionDefinition(
                        name=param_name,
                        description=annotation.description,
                        default=annotation.default,
                        required=annotation.required,
                        min=annotation.min,
                        max=annotation.max,
                        enum=annotation.enum,
                        type=OptionDefinition.Types.from_type(annotation.type),
                    )
                elif isinstance(annotation, Input):
                    metadata["input_slots"][param_name] = SlotDefinition(
                        name=param_name,
                        description=annotation.description,
                        required=annotation.required,
                        multiple=annotation.multiple,
                        type=annotation.type,
                    )
                elif isinstance(annotation, Output):
                    metadata["output_slots"][param_name] = SlotDefinition(
                        name=param_name,
                        description=annotation.description,
                        required=annotation.required,
                        multiple=annotation.multiple,
                        type=annotation.type,
                    )
                else:
                    raise ValueError(
                        f"Parameter {param_name} in the function signature has an unsupported annotation: {annotation}"
                    )
            else:
                if param_name in ("context", "ctx"):
                    metadata["context_varname"] = param_name
                raise ValueError(
                    f"Parameter {param_name} in the function signature is missing an annotation."
                )

        @functools.wraps(func)
        def wrapped_function(*args, **kwargs):
            return func(*args, **kwargs)

        metadata["runnable"] = wrapped_function
        component = Component(**metadata)
        wrapped_function.component = component
        wrapped_function.metadata = metadata
        return wrapped_function

    return decorator

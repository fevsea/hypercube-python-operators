import functools
import inspect
import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from inspect import Parameter
from typing import (
    Any,
    Callable,
    Type,
    Iterable,
    TypeAliasType,
    _AnnotatedAlias,
)

from pydantic import BaseModel, Field

from runtime.context import Context
from runtime.persistance import Datum, DatumDefinition


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


@dataclass
class SlotDefinition:
    """Describes an input/output slot.

    Two components can be connected if they have compatible IoSlot objects.
    """

    name: str
    description: str = ""
    required: bool = True
    multiple: bool = False
    type: DatumDefinition.Type = DatumDefinition.Type.FOLDER


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
            # This is not done with a dict lookup to avoid
            # polluting the enum options or moving the dict
            # outside the class.
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
        labels: Iterable[str] = None,
        context_varname: str = None,
        version: str = "1",
    ):
        # Metadata about the component
        self.name: str = name
        self.description: str = description
        self.labels: set[str] = set(labels) or set()
        self.version: str = version

        # Describes the specific I/O shape of the Component.
        self.input_slots: dict[str, SlotDefinition] = input_slots or {}
        self.output_slots: dict[str, SlotDefinition] = output_slots or {}
        self.available_options: dict[str, OptionDefinition] = options or {}

        # Calling ingo
        self.runnable: Callable = runnable
        self.context_varname: str = context_varname

    def run(
        self,
        context: Context,
        input_data: dict[str, Datum | None | list[Datum]],
        output_data: dict[str, Datum | None | list[Datum]],
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
#
# The most convenient way to define components is with the decorator
# It will internally build a Component object and associate it with the fn
# In this section lays the decorator and the types it uses.
###


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


def get_real_type(param) -> (type, tuple):
    """Navigate the possibly nested hierarchy of Aliasses and annotations"""
    if param is None:
        return tuple(), None
    metadata = tuple()
    if hasattr(param, "__metadata__"):
        metadata = param.__metadata__ or tuple()
    if isinstance(param, _AnnotatedAlias):
        new_metadata, param_type = get_real_type(param.__origin__)
        return metadata + new_metadata, param_type
    elif isinstance(param, TypeAliasType):
        return get_real_type(param.__value__)
    return metadata, param


def decode_param(param_name: str, param: Parameter, annotation) -> dict[str, Any]:
    """Read the metadata associated with the parmeter as well as it's type"""

    annotation_metadata, annotation_type = get_real_type(annotation)

    decoded = {
        "type": annotation_type,
        "name": param_name,
        "required": param.default == param.empty,
    }

    if not decoded["required"]:
        decoded["default"] = param.default

    for item in annotation_metadata:
        if isinstance(item, str):
            decoded[item] = True
            continue
        if hasattr(item, "gt"):
            if "min" in decoded:
                raise RuntimeError(
                    f"Annotation for {param_name} param already has a min value"
                )
            decoded["min"] = item.gt
        if hasattr(item, "ge"):
            if "min" in decoded:
                raise RuntimeError(
                    f"Annotation for {param_name} param already has a min value"
                )
            decoded["min"] = item.ge
        if hasattr(item, "lt"):
            if "max" in decoded:
                raise RuntimeError(
                    f"Annotation for {param_name} param already has a max value"
                )
            decoded["max"] = item.lt
        if hasattr(item, "le"):
            if "max" in decoded:
                raise RuntimeError(
                    f"Annotation for {param_name} param already has a max value"
                )
            decoded["max"] = item.le
        if hasattr(item, "documentation"):
            decoded["description"] = item.documentation

    return decoded


def command_component(
    name: str,
    version: str = "1",
    description: str = "",
    labels: Iterable[str] = None,
) -> Callable:
    """Decorator that defines a runnable component."""

    def decorator(func: Callable) -> Callable:
        if not callable(func) or not inspect.isfunction(func):
            raise TypeError(
                "The @command_component decorator can only be applied to functions."
            )

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

        # Extract the signature and annotations of the function
        signature = inspect.signature(func)
        annotations = func.__annotations__

        # Parse the function arguments using its annotations
        for param_name, param in signature.parameters.items():
            decoded_param = decode_param(
                param_name, param, annotations.get(param_name, None)
            )
            if decoded_param["type"] is not None and issubclass(
                decoded_param["type"], Datum
            ):
                if "input" in decoded_param:
                    metadata["input_slots"][param_name] = SlotDefinition(
                        name=decoded_param.get("name"),
                        description=decoded_param.get("description", ""),
                        required=decoded_param["required"],
                        multiple=False,
                        type=decoded_param["type"].io_type,
                    )
                else:
                    metadata["output_slots"][param_name] = SlotDefinition(
                        name=decoded_param["name"],
                        description=decoded_param.get("description", ""),
                        required=decoded_param["required"],
                        multiple=False,
                        type=decoded_param["type"].io_type,
                    )
            elif decoded_param["type"] == Context:
                metadata["context_varname"] = param_name
            elif decoded_param["type"] in (str, int, float, bool, datetime):
                metadata["options"][param_name] = OptionDefinition(
                    name=decoded_param["name"],
                    default=decoded_param.get("default", None),
                    description=decoded_param.get("description", ""),
                    required=decoded_param["required"],
                    min=decoded_param.get("min", None),
                    max=decoded_param.get("max", None),
                    enum=None,
                    type=OptionDefinition.Types.from_type(decoded_param["type"]),
                )
            elif decoded_param.get("type", None) is None and decoded_param["name"] in (
                "context",
                "ctx",
            ):
                metadata["context_varname"] = param_name
            else:
                raise ValueError(
                    f"Parameter {param_name} in the function signature has an unsupported annotation: {decoded_param}"
                )

        # if isinstance(arg, (annotated_types.BaseMetadata)

        @functools.wraps(func)
        def wrapped_function(*args, **kwargs):
            return func(*args, **kwargs)

        metadata["runnable"] = wrapped_function  # noqa
        component = Component(**metadata)
        wrapped_function.component = component
        wrapped_function.metadata = metadata
        return wrapped_function

    return decorator

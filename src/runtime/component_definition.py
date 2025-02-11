import functools
import inspect
import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from inspect import Parameter
from types import GenericAlias
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
from runtime.persistance import Datum, DatumDefinition, UnspecifiedDatum, DatumFactory


class TaskDefinition(BaseModel):
    """Defines a single executable operation.

    It only holds the definition, it cannot be executed by itself.
    """

    # Component info
    component: str
    library: str = "local"

    # Parameters
    options: dict[str, Any] = Field(default_factory=list)
    input_data: dict[str, DatumDefinition] = Field(default_factory=dict)
    output_data: dict[str, DatumDefinition] = Field(default_factory=dict)


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


type OptionTypes = str | int | float | bool


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

        @classmethod
        def cast_if_needed(
            cls, target_type: "OptionDefinition.Types", value: OptionTypes
        ):
            """Will try to cast between integer and float if possible. Any other cast results on an error."""
            value_type = cls.from_type(type(value))
            if target_type == value_type:
                return value

            if (
                target_type == cls.INTEGER
                and value_type == cls.FLOAT
                and int(value) == value
            ):
                return int(value)
            elif (
                target_type == cls.FLOAT
                and value_type == cls.INTEGER
                and float(value) == value
            ):
                return float(value)
            else:
                raise ValueError(
                    f"Cannot cast '{value}' from '{value_type}' type  to '{target_type}' type."
                )

    name: str
    description: str = ""
    type: Types = Types.STRING
    default: any = None
    required: bool = True
    min: None | int | float = None
    max: None | int | float = None
    enum: None | list[str] = None


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
        input_data: dict[str, Datum | None | list[Datum]] = None,
        output_data: dict[str, Datum | None | list[Datum]] = None,
        options: dict[str, OptionTypes] = None,
    ):
        """Actually executes the component with specific data.

        This is where most of the checks should happen, as it's only invoked on the operation we want to run.
        """

        # Check constraints, try to cast values, and handle optional parameters
        input_data = self._validate_inputs(input_data)
        output_data = self._validate_outputs(output_data)
        options = self._validate_options(options)

        params = {**input_data, **output_data, **options}
        if self.context_varname is not None:
            params[self.context_varname] = context
        self.runnable(**params)

    @staticmethod
    def _validate(category: str, provided, expected, additional_validator: callable):
        provided = provided or {}
        cleaned = {}

        for name, definition in expected.items():
            if name not in provided:
                if definition.required:
                    raise ValueError(
                        f"{category} '{name}' is required but not provided."
                    )
                else:
                    if hasattr(definition, "default"):
                        cleaned[name] = definition.default
            else:
                value = provided[name]
                value = additional_validator(name, value, definition)
                cleaned[name] = value

        for name in provided.keys():
            if name not in expected:
                raise Warning(
                    f"{category} '{name}' is not available for this component."
                )
        return cleaned

    def _validate_options(
        self, options: dict[str, OptionTypes] | None
    ) -> dict[str, OptionTypes]:
        """Make sure types are correct and within constraints. Adds options not present with defaults."""

        def extra_validation(name, value, definition):
            if definition.min is not None and value < definition.min:
                raise ValueError(f"Option '{name}' is too small.")
            if definition.max is not None and value > definition.max:
                raise ValueError(f"Option '{name}' is too big.")
            return OptionDefinition.Types.cast_if_needed(definition.type, value)

        return self._validate(
            "Option", options, self.available_options, extra_validation
        )

    def _validate_inputs(self, datums: dict[str, Datum]):

        def extra_validation(name, value, definition):
            if not isinstance(value, Iterable):
                # Temporally transform into list
                value = [value]
            casted_values = []  # Needed because we modify the list while iterating it
            for item in value:
                if item.io_type != definition.type:
                    if isinstance(item, UnspecifiedDatum):
                        item = item.promote(definition.type)
                    else:
                        raise ValueError(
                            f"Input '{name}' is of type '{item.io_type}' but should be '{definition.type}'."
                        )
                casted_values.append(item)
            if not definition.multiple:
                if len(casted_values) > 1:
                    raise ValueError(
                        f"Input '{name}' is marked as single but has multiple values."
                    )
                else:
                    casted_values = casted_values[0]
            return casted_values

        return self._validate("Input", datums, self.input_slots, extra_validation)

    def _validate_outputs(self, datums: dict[str, Datum]):

        def extra_validation(name, value: Datum, definition):
            if isinstance(value, UnspecifiedDatum):
                value = value.promote(definition.type)
            if definition.multiple and not isinstance(value, DatumFactory):
                    value = DatumFactory(value.get_definition())
            if value.io_type != definition.type:
                raise ValueError(
                    f"Output '{name}' is of type '{value.io_type}' but should be '{definition.type}'."
                )
            return value

        return self._validate("Output", datums, self.output_slots, extra_validation)

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
    elif isinstance(param, TypeAliasType) or isinstance(param, GenericAlias):
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

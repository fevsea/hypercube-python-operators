from typing import Annotated

from annotated_types import Gt, Lt
from typing_extensions import Doc

from runtime.component_definition import (
    SlotDefinition,
    command_component,
    Component,
    OptionDefinition,
)
from runtime.context import Context
from runtime.persistance import FolderDatum, FolderDatumInput, FolderDatumOutput


def test_command_component_metadata():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["test"],
    )
    def test_func():
        pass

    component: Component = test_func.component

    assert isinstance(component, Component)
    assert component.name == "test_component"
    assert component.version == "1.0"
    assert component.description == "A test command component"
    assert component.labels == {
        "test",
    }

    assert not component.input_slots
    assert not component.available_options
    assert not component.output_slots


def test_command_component_input():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(
        param1: Annotated[FolderDatumInput, Doc("A description")],
        param2: FolderDatumInput,
    ):
        pass

    component: Component = test_func.component
    assert not component.output_slots
    assert not component.available_options

    assert "param1" in component.input_slots
    param1: SlotDefinition = component.input_slots["param1"]
    assert isinstance(param1, SlotDefinition)
    assert param1.name == "param1"
    assert param1.type == SlotDefinition.type.FOLDER
    assert param1.required is True
    assert param1.multiple is False
    assert param1.description == "A description"

    assert "param2" in component.input_slots
    param1: SlotDefinition = component.input_slots["param2"]
    assert isinstance(param1, SlotDefinition)
    assert param1.name == "param2"
    assert param1.type == SlotDefinition.type.FOLDER
    assert param1.required is True
    assert param1.multiple is False
    assert param1.description == ""


def test_command_component_output():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(
        param1: Annotated[FolderDatumOutput, Doc("A description")],
        param2: FolderDatum,
    ):
        pass

    component: Component = test_func.component
    assert not component.input_slots
    assert not component.available_options

    assert "param1" in component.output_slots
    param1: SlotDefinition = component.output_slots["param1"]
    assert isinstance(param1, SlotDefinition)
    assert param1.name == "param1"
    assert param1.type == SlotDefinition.type.FOLDER
    assert param1.required is True
    assert param1.multiple is False
    assert param1.description == "A description"

    assert "param2" in component.output_slots
    param1: SlotDefinition = component.output_slots["param2"]
    assert isinstance(param1, SlotDefinition)
    assert param1.name == "param2"
    assert param1.type == SlotDefinition.type.FOLDER
    assert param1.required is True
    assert param1.multiple is False
    assert param1.description == ""


def test_command_component_no_context():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(
        param1: Annotated[FolderDatumOutput, Doc("A description")],
        param2: FolderDatum,
    ):
        pass

    component: Component = test_func.component
    assert component.context_varname is None


def test_command_component_implicit_context():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(
        context,
        param1: Annotated[FolderDatumOutput, Doc("A description")],
        param2: FolderDatum,
    ):
        pass

    component: Component = test_func.component
    assert component.context_varname == "context"


def test_command_component_explicit_context():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(
        myContext: Context,
        param1: Annotated[FolderDatumOutput, Doc("A description")],
        param2: FolderDatum,
    ):
        pass

    component: Component = test_func.component
    assert component.context_varname == "myContext"


def test_command_options():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(
        param1: int,
        param2: Annotated[str, Doc("A description"), Lt(10), Gt(0)],
        param3: float = 0.0,
    ):
        pass

    component: Component = test_func.component
    assert not component.input_slots
    assert not component.output_slots

    assert "param1" in component.available_options
    param1: OptionDefinition = component.available_options["param1"]
    assert isinstance(param1, OptionDefinition)
    assert param1.name == "param1"
    assert param1.type == OptionDefinition.type.INTEGER
    assert param1.required == True
    assert param1.default is None
    assert param1.description == ""
    assert param1.min is None
    assert param1.max is None

    assert "param2" in component.available_options
    param2: OptionDefinition = component.available_options["param2"]
    assert isinstance(param2, OptionDefinition)
    assert param2.name == "param2"
    assert param2.type == OptionDefinition.type.STRING
    assert param2.required == True
    assert param2.default is None
    assert param2.description == "A description"
    assert param2.min == 0
    assert param2.max == 10

    assert "param3" in component.available_options
    param3: OptionDefinition = component.available_options["param3"]
    assert isinstance(param3, OptionDefinition)
    assert param3.name == "param3"
    assert param3.type == OptionDefinition.type.FLOAT
    assert param3.required == False
    assert param3.default == 0.0
    assert param3.description == ""
    assert param3.min is None
    assert param3.max is None

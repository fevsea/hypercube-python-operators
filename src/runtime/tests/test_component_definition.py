from typing import Annotated

from annotated_types import Gt, Lt, Interval

from runtime.component_definition import (
    SlotDefinition,
    command_component,
    OptionDefinition,
    Option,
    Component,
    OptionDefinition,
    IoType,
)
from runtime.persistance import FolderDatum, FolderDatumInput, FolderDatumOutput


def test_command_component_metadata():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(param1: FolderDatumInput, param2: FolderDatumOutput, param3: int = 3):
        pass

    component: Component = test_func.component

    assert isinstance(component, Component)
    assert component.name == "test_component"
    assert component.version == "1.0"
    assert component.description == "A test command component"
    assert component.labels == {
        "1.0",
    }

    assert not component.input_slots  # Is empty
    assert "param1" in component.available_options
    assert "param2" in component.output_slots

    param1: OptionDefinition = component.available_options["param1"]
    param2 = component.output_slots["param2"]

    assert isinstance(param1, OptionDefinition)
    assert param1.type == OptionDefinition.Types.INTEGER
    assert param1.name == "param1"
    assert param1.default == 1

    assert isinstance(param2, SlotDefinition)
    assert param2.name == "param2"
    assert param2.type == IoType.FOLDER


def test_command_component_single_input():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(param1: Annotated[FolderDatumInput, Gt(2), Lt(10)]):
        pass

    component: Component = test_func.component

    assert not component.output_slots
    assert not component.available_options

    assert "param1" in component.input_slots
    param1: SlotDefinition = component.input_slots["param1"]
    assert isinstance(param1, SlotDefinition)
    assert param1.name == "param1"
    assert param1.type == IoType.FOLDER
    assert param1.required is True
    assert param1.multiple is False
    assert param1.description == ""

import pytest
from runtime.component_definition import command_component, Input, Output, Option


def test_command_component_metadata():
    @command_component(
        name="test_component",
        version="1.0",
        description="A test command component",
        labels=["1.0"],
    )
    def test_func(param1: Option(type=int), param2: Output()):
        pass

    metadata = test_func.metadata

    assert metadata["name"] == "test_component"
    assert metadata["version"] == "1.0"
    assert metadata["display_name"] == "Test Component"
    assert metadata["description"] == "A test command component"
    assert metadata["input_slots"] == [{"name": "param1", "type": Input.type}]
    assert metadata["output_slots"] == [{"name": "param2", "type": Output.type}]


def test_command_component_without_optional_metadata():
    @command_component(name="minimal_component", version="1.0")
    def another_func():
        pass

    metadata = another_func.metadata

    assert metadata["name"] == "minimal_component"
    assert metadata["version"] == "1.0"
    assert metadata["display_name"] == ""
    assert metadata["description"] == ""
    assert metadata["input_slots"] == []
    assert metadata["output_slots"] == []

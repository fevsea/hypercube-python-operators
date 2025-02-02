import pytest
from pydantic import BaseModel

from runtime.catalog_base import Catalog
from runtime.component_definition import (
    Component,
    ComponentTags,
    SlotDefinition,
    IoType,
)
from runtime.persistance import ObjectDatum


class DummyComponent(Component):
    meta_name: str = "dummy_component"
    meta_labels: tuple[str] = {ComponentTags.IMPORTER}

    meta_input_slots: tuple[SlotDefinition] = (
        SlotDefinition(
            name="in",
            required=False,
            multiple=False,
            type=IoType.OBJECT,
        ),
    )
    meta_output_slots: tuple[SlotDefinition] = (
        SlotDefinition(
            name="out",
            required=False,
            multiple=False,
            type=IoType.OBJECT,
        ),
    )

    class Options(Component.Options):
        int_option: int | None = None
        str_option: str | None = None

    def run(self):
        in_data: ObjectDatum = self.input_data[0]
        out_data: ObjectDatum = self.output_data[0]

        if in_data is not None and out_data is not None:
            out_data.set_object(out_data.get_object())


@pytest.fixture(scope="session")
def dummy_component():
    return DummyComponent


@pytest.fixture(scope="session")
def dummy_catalog(dummy_component):
    return Catalog(
        name="python-component-catalog",
        description="Basic components written in Python",
        components=[
            dummy_component,
        ],
    )

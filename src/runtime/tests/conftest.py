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
    name: str = "dummy_component"
    labels: tuple[str] = {ComponentTags.IMPORTER}

    input_slots: tuple[SlotDefinition] = (
        SlotDefinition(
            name="in",
            required=False,
            multiple=False,
            type=IoType.OBJECT,
        ),
    )

    output_slots: tuple[SlotDefinition] = (
        SlotDefinition(
            name="out",
            required=False,
            multiple=False,
            type=IoType.OBJECT,
        ),
    )

    def run(self):
        in_data: ObjectDatum = self.input_data[0]
        out_data: ObjectDatum = self.output_data[0]

        if in_data is not None and out_data is not None:
            out_data.set_object(out_data.get_object())


@pytest.fixture(scope="session")
def dummy_component():
    # Todo: Fix
    return None


@pytest.fixture(scope="session")
def dummy_catalog(dummy_component):
    return Catalog(
        name="",
        description="Basic components written in Python",
        components=[
            dummy_component,
        ],
    )

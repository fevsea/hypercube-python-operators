import pytest

from runtime.catalog_base import Catalog
from runtime.component_definition import (
    ComponentTags,
    command_component,
)
from runtime.persistance import ObjectDatum, ObjectDatumOutput


@command_component(
        name="dummy_component",
        version="1.0",
        description="A test command component",
        labels=(ComponentTags.IMPORTER, ),
    )
def run(in_data: ObjectDatum=None, out_data: ObjectDatumOutput=None):
    if in_data is not None and out_data is not None:
        out_data.set_object(out_data.get_object())


@pytest.fixture(scope="session")
def dummy_component():
    return run.component


@pytest.fixture(scope="session")
def dummy_catalog(dummy_component):
    return Catalog(
        name="",
        description="Basic components written in Python",
        components=[
            dummy_component,
        ],
    )

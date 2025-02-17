from pathlib import Path

import pytest
from hypercube.runtime.component_definition import Component, TaskDefinition
from hypercube.runtime.context import Context
from hypercube.runtime.persistance import DatumFactory, DatumDefinition, FolderDatum

from market_importer.market_importer import market_importer


@pytest.fixture
def component() -> Component:
    return Component.from_decorated(market_importer)

@pytest.fixture
def output_data(tmp_path: Path):
    template = DatumDefinition(
        path=tmp_path,
        type=DatumDefinition.Type.DATAFRAME
    )
    return DatumFactory(template)

@pytest.fixture
def run_context():
    return Context(None, TaskDefinition(component="test"))

def test_instantiation(component):
    """Test instantiation of market_importer"""
    assert component is not None
    assert component.name == "market_importer"

@pytest.mark.xfail(reason="Not implemented yet")
def test_run(component, output_data, shared_datadir, run_context):
    input_data = FolderDatum(DatumDefinition(path=shared_datadir/"minutes_zip", type=DatumDefinition.Type.FOLDER))
    market_importer(context=run_context, raw=input_data, datum_factory=output_data)
    a = 1

import pytest

from runtime.component_definition import Component
from .market_importer import market_importer


@pytest.fixture
def component() -> Component:
    return Component.from_decorated(market_importer)

def test_instantiation(component):
    """Test instantiation of market_importer"""
    assert component is not None
    assert component.name == "market_importer"

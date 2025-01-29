from catalog import catalog
from runtime.component_definition import Component


def test_all_components_are_of_the_correct_type():
    # Since this static data we can check the constraints on tests rather than at runtime
    for component in catalog.components.values():
        assert isinstance(component, type) and issubclass(component, Component)

from catalog import catalog
from runtime.operator_definition import Operator


def test_all_operators_are_of_the_correct_type():
    # Since this static data we can check the constraints on tests rather than at runtime
    for operator in catalog.operators.values():
        assert isinstance(operator, type) and issubclass(operator, Operator)

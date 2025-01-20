from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Type


from runtime.operator_definition import Operator

class Catalog:
    """Represent a group of algorithms on a single sourcecode.

    For an operator to be usable, it needs to extend the Operator class and be registered in the catalog.
    Even when considering the redundancy and opportunity for user error, it has been deemed better than:
    - scanning the library at runtime (too much overhead)
    - having a separate code analysis step (would need to be added),
    - a more complex import system based on project-specific conventions (not obvious)
    """

    def __init__(
        self, name: str, operators: list[Type[Operator]], description: str = ""
    ):
        """Library name (as id)"""
        self.name: str = name
        self.description: str = description
        self.operators: Dict[str, Type[Operator]] = {op.meta_name: op for op in operators}

    def get_operator(self, operation_definition: OperationDefinition):
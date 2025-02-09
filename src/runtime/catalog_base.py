from typing import Dict, Type

from runtime.component_definition import Component, TaskDefinition


class Catalog:
    """Represent a group of algorithms on a single sourcecode.

    For a component to be usable, it needs to extend the Component class and be registered in the catalog.
    Even when considering the redundancy and opportunity for user error, it has been deemed better than:
    - scanning the library at runtime (too much overhead)
    - having a separate code analysis step (would need to be added),
    - a more complex import system based on project-specific conventions (not obvious)
    """

    def __init__(
        self, name: str, components: list[Component], description: str = ""
    ):
        """Library name (as id)"""
        self.name: str = name
        self.description: str = description
        self.components: Dict[str, Component] = {
            component.name.lower(): component for component in components
        }

    def get_component(self, name: str, library: str | None = "") -> Component:
        """
        Retrieve a specific component from the current task.

        If the library parameter is None, it is not check
        """
        if library is not None and library.lower() != self.name.lower():
            raise ValueError(
                f"The task uses a component from another library '{library}'"
            )
        cpn_name = name.lower()
        if cpn_name not in self.components:
            raise ValueError(f"The task uses an unknown component '{name}'")
        return self.components[cpn_name.lower()]

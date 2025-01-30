import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from runtime.runtimes import Runtime, TaskDefinition


class Context:
    """Object passed to Component that allows them to interact with the runtime.

    It is mainly a facade class for the runtime that simplifies the interface while ensuring users don't mess
    with the runtime directly.

    This API should be very stable, so introducing a layer of indirection will simplify changes.
    """

    def __init__(self, runtime: "Runtime", task_definition: "TaskDefinition"):
        self._runtime: "Runtime" = runtime
        self._task_definition: "TaskDefinition" = task_definition

    def get_logger(self, name: str = ""):
        logger_name = self._task_definition.component
        if name != "":
            logger_name = f"{logger_name}.{name}"
        return logging.getLogger(logger_name)

import abc

import typing
import asyncio

from runtime.communication import CommunicationBackend


class Runtime:
    """Handles the execution of tasks assigned by the cubelet."""

    def __init__(self, communication_backend: CommunicationBackend):
        self.communication_backend: CommunicationBackend = communication_backend

    def start(self):
        """Start executing tasks."""


class Context:
    """Object passed to Operators that allows them to interact with the runtime.

    It is mainly a facade class for the runtime that simplifies the interface while ensuring users don't mess
    with the runtime directly.

    This API should be very stable, so introducing a layer of indirection will simplify changes.
    """

    def __init__(self, runtime: Runtime):
        self._runtime: Runtime = runtime

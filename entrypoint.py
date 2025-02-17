"""Entrypoint for the application. This should be as shallow as possible."""

from hypercube.runtime.communication import SimpleCliCommunicationBackend
from hypercube.runtime.runtimes import Runtime

from catalog import catalog

if __name__ == "__main__":
    communication_backend = SimpleCliCommunicationBackend(catalog)
    runtime = Runtime(catalog, communication_backend)
    runtime.start()

"""Entrypoint for the application. This should be as shallow as possible."""

from catalog import catalog
from runtime.communication import SimpleCliCommunicationBackend
from runtime.runtimes import Runtime

if __name__ == "__main__":
    communication_backend = SimpleCliCommunicationBackend(catalog)
    runtime = Runtime(catalog, communication_backend)
    runtime.start()

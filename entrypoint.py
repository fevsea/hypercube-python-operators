"""Entrypoint for the application. This should be as shallow as possible."""

from runtime.communication import TerminalCommunicationBackend
from runtime.runtimes import Runtime

if __name__ == "__main__":
    communication_backend = TerminalCommunicationBackend()
    runtime = Runtime(communication_backend)
    runtime.start()

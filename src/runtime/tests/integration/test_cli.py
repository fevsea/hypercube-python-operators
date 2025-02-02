import pickle
import sys
from unittest.mock import patch

from catalog import catalog
from runtime.communication import SimpleCliCommunicationBackend
from runtime.runtimes import Runtime
import pytest


@pytest.fixture()
def cli_runtime(dummy_catalog):
    communication_backend = SimpleCliCommunicationBackend()
    return Runtime(catalog, communication_backend)


@pytest.fixture(scope="session")
def input_datum(tmp_path_factory):
    fn_path = tmp_path_factory.mktemp("input_datum") / "data.pkl"
    with open(fn_path, "wb") as file:
        pickle.dump("Hello", file)

    # Provide the file path to the test functions
    yield fn_path

    # Cleanup after test session
    fn_path.unlink(missing_ok=True)


def test_cli_catalog(runtime, input_datum, tmp_path):
    with patch.object(
        sys,
        "argv",
        [
            "my_program.py",
            "dummy_component",
            "-i",
            input_datum,
            "-o",
            tmp_path,
        ],
    ):
        runtime.start()

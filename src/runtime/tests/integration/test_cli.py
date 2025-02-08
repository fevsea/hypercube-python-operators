import pickle
import sys
from unittest.mock import patch

import pytest

from runtime.communication import SimpleCliCommunicationBackend
from runtime.runtimes import Runtime


@pytest.fixture(scope="session")
def input_datum(tmp_path_factory):
    folder_path = tmp_path_factory.mktemp("input_datum")
    fn_path = folder_path / "data.pkl"
    with open(fn_path, "wb") as file:
        pickle.dump("Hello", file)

    # Provide the folder path to the test functions
    yield folder_path

    # Cleanup after test session
    fn_path.unlink(missing_ok=True)


def test_cli_catalog(input_datum, tmp_path, dummy_catalog):
    with patch.object(
        sys,
        "argv",
        [
            "my_program.py",
            "dummy_component",
            "-i",
            str(input_datum),
            "-o",
            str(tmp_path),
        ],
    ):
        communication_backend = SimpleCliCommunicationBackend()
        cli_runtime = Runtime(dummy_catalog, communication_backend)
        cli_runtime.start()

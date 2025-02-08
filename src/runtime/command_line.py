import argparse
import json
import tomllib
from pathlib import Path

import yaml


#!
#! THIS FILE IS PART OF THE SHARED LIBRARY, ONCE THAT IS PUBLISHED THIS SHOULD BE REMOVED
#!


class FileLoaderMultiExtension:
    """A class for loading and decoding data files with various extensions.

    Methods:
        get_data(): Retrieves the decoded data. If it has not been loaded yet, it will be loaded first.

    Usage:
        FileLoaderMultiExtension(path).get_data()


    """

    def __init__(self, path: str | Path):
        self.path: Path = Path(path)
        self._data: dict | None = None

    def get_data(self):
        if self._data is None:
            self._load_data()
        return self._data

    def _load_data(self):
        extension_mapping = {
            ".yaml": self._decode_yaml,
            ".yml": self._decode_yaml,
            ".toml": self._decode_toml,
            ".json": self._decode_json,
        }

        if not self.path.is_file():
            raise FileNotFoundError
        extension = self.path.suffix.lower()
        extension_mapping[extension]()

    def _decode_yaml(self):
        self._data = yaml.safe_load(self.path)

    def _decode_toml(self):
        with self.path.open("rb") as f:
            self._data = tomllib.load(f)

    def _decode_json(self):
        with self.path.open("r") as f:
            self._data = json.load(f)


def kv_pairs(string) -> dict[str, str]:
    """Argparse type that parses the format "keyA=valueA,keyB=valueB" into a dictionary."""
    try:
        return dict(kv.strip().split("=") for kv in string.split(","))
    except ValueError as exc:
        msg = "comma-separated key=value pairs expected"
        raise argparse.ArgumentTypeError(msg) from exc


def parse_kv_pairs(input_kv):
    """Given a list of dicts, it merges them into a single dict.

    It takes care of constructing a nested structure when the key includes "." or "__" as separator.
    :param input_kv:
    :return:
    """
    if input_kv is None:
        input_kv = []
    parsed = {}
    for options_dict in input_kv:
        if options_dict is None:
            continue
        for k, v in options_dict.items():
            k = k.replace("__", ".")  # noqa: PLW2901
            root = parsed
            hierarchy = k.split(".")
            for level_name in hierarchy[:-1]:
                if level_name not in root:
                    root[level_name] = {}
                root = root[level_name]
            root[hierarchy[-1]] = v

    return parsed

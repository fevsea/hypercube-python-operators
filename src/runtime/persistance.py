from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, TextIO
from unittest import case

import pandas as pd
from pydantic import BaseModel, Field
import pickle
import json
import yaml
import tomllib


class DatumDefinition(BaseModel):
    """Describes a datum that can be used in a Component."""

    class Config:
        extra = "allow"

    class Type(StrEnum):
        FILE = "file"
        FOLDER = "folder"
        DATAFRAME = "dataframe"
        OBJECT = "object"

    path: Path
    type: Type = Field(default=Type.FILE)
    hash: str | None = None


class Datum:
    """Represents a unit of data on the cluster.

    Only this class can be actually persisted and retrieved from the disk. Other classes are just wrappers.
    """

    def __init__(self, datum_definition: DatumDefinition):
        """Only uncommitted data can be modified."""
        self._definition = datum_definition

    @classmethod
    def datum_factory(cls, datum_definition) -> "Datum":
        """Creates the appropriate class for the given type"""
        match datum_definition.type:
            case DatumDefinition.Type.FILE:
                return FileDatum(datum_definition)
            case DatumDefinition.Type.FOLDER:
                return FolderDatum(datum_definition)
            case DatumDefinition.Type.DATAFRAME:
                return DataFrameDatum(datum_definition)
            case DatumDefinition.Type.OBJECT:
                return ObjectDatum(datum_definition)
            case _:
                raise ValueError(f"Unknown datum type: {datum_definition.type}")


class FolderDatum(Datum):
    """Represents a folder on disk."""

    def get_path(self) -> Path:
        return self._definition.path


class FileDatum(Datum):
    """Represents a file on disk.

    We don't offer the full path to make sure the data is always opened as read-only.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filename = self._find_single_file()

    def get_filename(self) -> str:
        """Gets the filename from the path, including extension if present."""
        return self.filename

    def open_binary(self) -> BinaryIO:
        """Opens the file in binary mode."""
        if self.filename is None:
            raise RuntimeError("No single file inside the datum.")
        return (self._definition.path / self.filename).open("rb")

    def open(self) -> TextIO:
        """Opens the file in text mode."""
        if self.filename is None:
            raise RuntimeError("No single file inside the datum.")
        return (self._definition.path / self.filename).open("r")

    def _find_single_file(self):
        """Scans the path and returns the filename.

        The path is expected to be a folder and have a single file that doesn't start with dot
        """
        files = [
            file
            for file in self._definition.path.iterdir()
            if file.is_file() and not file.name.startswith(".")
        ]
        if len(files) == 0:
            return None
        elif len(files) > 1:
            raise ValueError(
                "The folder must contain exactly one file that does not start with a dot."
            )
        else:
            return files[0].name


class DataFrameDatum(FileDatum):
    """Represents a dataframe on disk. It is a special case of FileDatum provided for convenience."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._df = None

    def get_df(self):
        """Returns the dataframe."""
        if self._df is None:
            self._df = pd.read_parquet(self.open_binary())
        return self._df

    def clear(self):
        """Remove the dataframe from memory."""
        self._df = None


class ObjectDatum(FileDatum):
    """Returns an arbitrary python object.

    Known formats are pickle, json, toml, and yaml.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._object = None

    def get_object(self):
        """Returns the loaded object."""
        if self._object is None:
            ext = self.filename.split(".")[-1].lower()
            with self.open_binary() as file:
                if ext == "pickle":
                    self._object = pickle.load(file)
                elif ext == "json":
                    self._object = json.load(file)
                elif ext == "yaml" or ext == "yml":
                    self._object = yaml.safe_load(file)
                elif ext == "toml":
                    self._object = tomllib.load(file)
                else:
                    # Ideally, we shouldn't reach this point
                    self._object = pickle.load(file)
        return self._object

    def clear(self):
        """Remove the object from memory."""
        self._object = None

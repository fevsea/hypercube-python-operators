from enum import StrEnum
from io import TextIOWrapper
from pathlib import Path
from typing import BinaryIO, TextIO

from pydantic import BaseModel, Field


class DatumDefinition(BaseModel):
    """Describes a datum that can be used in an Operator."""

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
        self.commited = False
        self._definition = datum_definition


class FolderDatum(Datum):
    """Represents a folder on disk."""

    def get_path(self) -> Path:
        return self._definition.path


class FileDatum(Datum):
    """Represents a file on disk.

    We don't offer the full path to make sure the data is always opened as read-only.
    """

    def get_filename(self) -> str:
        """Gets the filename from the path, including extension if present."""
        return self._definition.path.name

    def open_binary(self) -> BinaryIO:
        """Opens the file in binary mode."""
        return self._definition.path.open("rb")

    def open(self) -> TextIO:
        """Opens the file in text mode."""
        return self._definition.path.open("r")


class DataFrameDatum(FileDatum):
    """Represents a dataframe on disk. It is a special case of FileDatum provided for convenience."""


class NilDatum(Datum):
    """Represents the absence of data."""

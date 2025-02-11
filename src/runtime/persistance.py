import json
import pickle
import tomllib
import uuid
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, TextIO, override, Annotated

import pandas as pd
import tomlkit
import yaml
from pydantic import BaseModel, Field


class DatumDefinition(BaseModel):
    """Describes a datum that can be used in a Component."""

    class Config:
        extra = "allow"

    class Type(StrEnum):
        FILE = "file"
        FOLDER = "folder"
        DATAFRAME = "dataframe"
        OBJECT = "object"
        NOT_YET_KNOWN = "not_yet_known"

    path: Path
    type: Type = Field(default=Type.FILE)
    hash: str | None = None


type DatumInput = Annotated[Datum, "input"]
type DatumOutput = Annotated[Datum, "output"]


class Datum:
    """Represents a unit of data on the cluster.

    Only this class can be actually persisted and retrieved from the disk. Other classes are just wrappers.
    """

    io_type: DatumDefinition.Type = DatumDefinition.Type.NOT_YET_KNOWN

    def __init__(self, datum_definition: DatumDefinition):
        """Only uncommitted data can be modified."""
        self._definition = datum_definition
        self._committed = False

    def get_definition(self) -> DatumDefinition:
        return self._definition

    def get_type(self) -> DatumDefinition.Type:
        return self._definition.type

    def is_committed(self) -> bool:
        return self._committed

    @classmethod
    def from_definition(cls, datum_definition: DatumDefinition) -> "Datum":
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
            case DatumDefinition.Type.NOT_YET_KNOWN:
                return UnspecifiedDatum(datum_definition)
            case _:
                raise ValueError(f"Unknown datum type: {datum_definition.type}")


type FolderDatumInput = Annotated[FolderDatum, "input"]
type FolderDatumOutput = Annotated[FolderDatum, "output"]


class FolderDatum(Datum):
    """Represents a folder on disk."""

    io_type: DatumDefinition.Type = DatumDefinition.Type.FOLDER

    def get_path(self) -> Path:
        return self._definition.path

    def commit_folder(self):
        self._committed = True


type UnspecifiedDatumInput = Annotated[UnspecifiedDatum, "input"]
type UnspecifiedDatumOutput = Annotated[UnspecifiedDatum, "output"]


class UnspecifiedDatum(Datum):
    """Represent a datum to which we don't know the type yet.

    Usually acts as a placeholder to indicate where a real datum should be saved.
    In order to do anything meaningful with this datum it must be promoted to another type.
    """

    io_type: DatumDefinition.Type = DatumDefinition.Type.NOT_YET_KNOWN

    def promote(self, new_type: DatumDefinition.Type):
        """Cast to one of the subclasses of Datum."""
        definition = self._definition.model_copy()
        definition.type = new_type
        return Datum.from_definition(self._definition)

    @override
    def get_type(self):
        return None


type FileDatumInput = Annotated[FileDatum, "input"]
type FileDatumOutput = Annotated[FileDatum, "output"]


class FileDatum(Datum):
    """Represents a file on disk.

    We don't offer the full path to make sure the data is always opened as read-only.
    """

    io_type: DatumDefinition.Type = DatumDefinition.Type.FILE

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

    def commit_file(self):
        """Marks the file as committed."""
        self._committed = True


type DataFrameDatumInput = Annotated[DataFrameDatum, "input"]
type DataFrameDatumOutput = Annotated[DataFrameDatum, "output"]


class DataFrameDatum(FileDatum):
    """Represents a dataframe on disk. It is a special case of FileDatum provided for convenience."""

    io_type: DatumDefinition.Type = DatumDefinition.Type.DATAFRAME

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._df = None

    def get_df(self):
        """Returns the dataframe."""
        if self._df is None:
            self._df = pd.read_parquet(self.open_binary())
        return self._df

    def set_df(self, df) -> "DataFrameDatum":
        if self._committed or self._df is not None:
            raise RuntimeError("Cannot modify data of an already committed datum.")
        self._df = df
        self._save_df()

    def clear(self):
        """Remove the dataframe from memory."""
        self._df = None

    def _save_df(self):
        filename = f"df.parquet"
        file_path = self._definition.path / filename
        self._df.to_parquet(file_path)


type ObjectDatumInput = Annotated[ObjectDatum, "input"]
type ObjectDatumOutput = Annotated[ObjectDatum, "output"]


class ObjectDatum(FileDatum):
    """Returns an arbitrary python object.

    Known formats are pickle, json, toml, and yaml.
    """

    io_type: DatumDefinition.Type = DatumDefinition.Type.OBJECT

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._object = None
        self.extension = "pkl"

    def get_object(self):
        """Returns the loaded object."""
        if self._object is None:
            ext = self.filename.split(".")[-1].lower()
            with self.open_binary() as file:
                if ext in ("pkl", "pickle"):
                    self._object = pickle.load(file)
                elif ext == "json":
                    self._object = json.load(file)
                elif ext == "yaml" or ext == "yml":
                    self._object = yaml.safe_load(file)
                elif ext == "toml":
                    self._object = tomllib.load(file)
                else:
                    # Ideally, we shouldn't reach this point
                    ext = "pkl"
                    self._object = pickle.load(file)
            self.extension = ext
        return self._object

    def clear(self):
        """Remove the object from memory."""
        self._object = None

    def set_object(self, data) -> "ObjectDatum":
        if self._committed:
            raise RuntimeError("Cannot modify data of an already committed datum.")
        self._object = data
        self._save_object()

    def _save_object(self):
        """Saves the object into disk. Supported formats are pickle, json, toml, and yaml."""
        filename = f"object.{self.extension}"
        file_path = self._definition.path / filename

        with file_path.open("wb" if self.extension == "pkl" else "w") as file:
            if self.extension in "pkl":
                pickle.dump(self._object, file)
            elif self.extension == "json":
                json.dump(self._object, file)
            elif self.extension in ("yaml", "yml"):
                yaml.safe_dump(self._object, file)
            elif self.extension == "toml":
                tomlkit.dump(self._object, file)
            else:
                raise ValueError(f"Unsupported file extension: {self.extension}")
        self._committed = True


type DatumFactoryOutput[T: Datum] = Annotated[DatumFactory[T: Datum], "output"]
class DatumFactory[T: Datum](Datum):
    """Special type of datums that allow for the creation of multiple datums.

    The datum definition acts as a template to create multiple ones.
    """

    # This is overridden on each instance
    io_type: DatumDefinition.Type = DatumDefinition.Type.NOT_YET_KNOWN

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._generated_datums: list[Datum] = []
        self.io_type = self._definition.type

    def create_datum(self) -> T:
        new_definition = self._definition.model_copy()
        new_definition.path = self._definition.path / uuid.uuid4().hex
        new_datum = Datum.from_definition(new_definition)
        self._generated_datums.append(new_datum)
        return new_datum

    def get_generated_datums(self) -> list[T]:
        """Gets the generated datums with data."""
        return [datum for datum in self._generated_datums if datum.is_committed()]

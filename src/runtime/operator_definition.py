import abc
import logging
from enum import StrEnum
import re
from pathlib import Path

from pandas import DataFrame
from pydantic import BaseModel, Field


class OperatorTags(StrEnum):
    """Enum with some common tags.

    It's not an exhaustive list, as tags are just arbitrary strings.
    An enum is a convenient way to suggest common tags to devs.
    """

    # Role
    IMPORTER = "importer"
    EXPORTER = "exporter"
    TRANSFORMER = "transformer"
    ANALYZER = "analyzer"
    VISUALIZER = "visualizer"
    MODEL = "model"
    SIMULATOR = "simulator"
    EVALUATOR = "evaluator"

    # Formats
    RAW = "raw"
    TIMESERIES = "timeseries"


class IoType(StrEnum):
    FILE = "file"
    FOLDER = "folder"
    DATAFRAME = "dataframe"
    OBJECT = "object"


class SlotDefinition(BaseModel):
    """Describes an input/output slot.

    Two operators can be connected if they have compatible IoSlot objects.
    """

    name: str = Field(default="")
    tags: set[str] = tuple()

    required: bool = Field(default=True)
    # Whether the slot expects a list or a single instance of the declared type
    multiple: bool = Field(default=False)
    # What is the underlying data format expected.
    type: IoType = Field(default=IoType.FOLDER)


class SlotData:
    """Acts as a container for data that is passed between operators.

    It hides the details of how the data is stored, allowing for loading data from disk or memory.
    """

    def __init__(self, read_only: bool = False):
        self.read_only = read_only


pathlike = str | Path


class FolderSlot(SlotData):
    """
    Represents a folder on disk.

    It's the most generic type of SlotData, as it makes no assumptions over the format of the datum.
    """

    def __init__(self, path: pathlike, read_only: bool = False):
        super().__init__(read_only)
        self.path: Path = self.parse_pathlike_folder(path)

    @staticmethod
    def parse_pathlike_folder(path: pathlike) -> Path:
        if isinstance(path, Path):
            potential_path = path
        else:
            potential_path = Path(path)

        if not potential_path.exists() or not potential_path.is_dir():
            raise ValueError(
                f"The provided path '{path}' is not a valid path or does not exist."
            )
        return potential_path

    def get_path(self) -> Path:
        return self.path


class DataframeSlot(SlotData):
    def __init__(
        self,
        df: DataFrame | None,
        metadata: dict | None = None,
        read_only: bool = False,
    ):
        super().__init__(read_only)
        self.df = df
        self.is_materialized = False

        self.metadata = dict() if metadata is None else metadata

    def get_df(self) -> DataFrame:
        if self.df is None:
            raise ValueError(
                "The dataframe is missing. Ensure the slot contains a valid dataframe before accessing it."
            )
        return self.df


class Operator(abc.ABC):
    """Define the interface for an operator."""

    # Metadata about the operator
    meta_name: str = "GenericOperator"
    meta_labels: set[str] = set()

    # Describes the specific I/O shape of the Operator.
    meta_input_slots: tuple[SlotDefinition, ...] = tuple()
    meta_output_slots: tuple[SlotDefinition, ...] = tuple()

    class Options(BaseModel):
        """An operator can have arbitrary options.

        This is ideally a single-level object. By using a Pydantic model, we can define its range.
        """

        pass

    def __init__(self, input_data: tuple[SlotData], options: Options):
        # Anz instance is tied to actual data and parameters
        self.input_data = input_data
        self.options = options
        self.logger = logging.getLogger(self.meta_name)

        # Check we have all required fields
        if len(self.meta_input_slots) != len(input_data):
            raise ValueError(
                f"The number of input slots ({len(input_data)}) does not match the number of required input slots ({len(self.meta_input_slots)})"
            )
        for slot, data in zip(self.meta_input_slots, input_data):
            if slot.required and data is None:
                raise ValueError(f"Slot {slot.name} is required but not provided.")

    def run(self) -> tuple[SlotData]:
        pass

    def get_human_name(self):
        """Return a human-readable name for the operator."""
        return re.sub(r"\W+", " ", self.meta_name)


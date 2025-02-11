from pathlib import Path

from pydantic.v1.dataclasses import DataclassProxy

from market_importer.stratergies import (
    ImportStrategy,
    MultipleFolderImporter,
    MultipleFilesImporter,
)
from runtime.component_definition import (
    Component,
    ComponentTags,
    SlotDefinition, command_component,
)
from runtime.context import Context
from runtime.enums import is_valid_currency_pair
from runtime.persistance import Datum, FolderDatumInput, DatumFactoryOutput, DataFrameDatum


@command_component(
    name="market_importer",
    version="1.0",
    description="A test command component",
    labels={ComponentTags.IMPORTER, ComponentTags.TIMESERIES},
)
def market_importer(context: Context, raw: FolderDatumInput, datum_factory: DatumFactoryOutput[DataFrameDatum]):
    root_folder_path: Path = raw.get_path()
    import_strategy: ImportStrategy | None = None

def run() -> tuple[Datum]:
    self.parse_folder_structure()
    for dfs in self.import_strategy.collect_iter():
        characterize_data(dfs)
        self.runtime.persist_slot(dfs)

def parse_folder_structure(self):
    """Inspect the folder to determine how many individual datasets there are.

    The scan is not recursive to limit simplify the logic. One import job should only have one type of data,
    allowing to dynamically change the import strategy might case problems with files that are part of the
    folder but not part of the datasets.
    """

    matching_subfolders = []
    pair_files = []
    non_pair_files = []

    for subpath in self.root_folder_path.iterdir():
        if subpath.is_dir() and is_valid_currency_pair(subpath.name):
            matching_subfolders.append(subpath)
        elif subpath.is_file() and subpath.suffix in (".parquet", ".csv"):
            if is_valid_currency_pair(subpath.stem):
                pair_files.append(subpath)
            else:
                non_pair_files.append(subpath)

    self.logger.debug(f"Found {len(matching_subfolders)} matching subfolders.")
    self.logger.debug(f"Found {len(pair_files)} pair files.")
    self.logger.debug(f"Found {len(non_pair_files)} non-pair files.")

    max_length = max(len(matching_subfolders), len(pair_files), len(non_pair_files))
    if len(matching_subfolders) == max_length:
        self.logger.info("Treating it as a folder of datasets.")
        self.import_strategy = MultipleFolderImporter(matching_subfolders)
    elif len(pair_files) == max_length:
        self.logger.info("Treating it as files of pairs.")
        self.import_strategy = MultipleFilesImporter(pair_files)
    elif len(non_pair_files) == max_length:
        self.logger.info("Treating it as files of non-pairs.")
        self.import_strategy = MultipleFilesImporter(non_pair_files)
    else:
        raise ValueError("The folder structure is not consistent.")

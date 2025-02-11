from pathlib import Path

from market_importer.stratergies import (
    ImportStrategy,
    MultipleFolderImporter,
    MultipleFilesImporter,
)
from runtime.component_definition import (
    ComponentTags, command_component,
)
from runtime.context import Context
from runtime.enums import is_valid_currency_pair
from runtime.persistance import FolderDatumInput, DatumFactoryOutput, DataFrameDatum


@command_component(
    name="market_importer",
    version="1.0",
    description="A test command component",
    labels={ComponentTags.IMPORTER, ComponentTags.TIMESERIES},
)
def market_importer(context: Context, raw: FolderDatumInput, datum_factory: DatumFactoryOutput[DataFrameDatum]):
    logger = context.get_logger()
    root_folder_path: Path = raw.get_path()

    stratergy: ImportStrategy = determine_root_stratergy(root_folder_path, logger)

    for df in stratergy.collect_iter():
        characterize_data(dfs)
        datum_factory.create_datum().set_df(df).clear()

def determine_root_stratergy(root_folder_path: Path, logger):
    """Inspect the folder to determine how many individual datasets there are.

    The scan is not recursive to limit simplify the logic. One import job should only have one type of data,
    allowing to dynamically change the import strategy might case problems with files that are part of the
    folder but not part of the datasets.
    """

    matching_subfolders = []
    pair_files = []
    non_pair_files = []

    for subpath in root_folder_path.iterdir():
        if subpath.is_dir() and is_valid_currency_pair(subpath.name):
            matching_subfolders.append(subpath)
        elif subpath.is_file() and subpath.suffix in (".parquet", ".csv"):
            if is_valid_currency_pair(subpath.stem):
                pair_files.append(subpath)
            else:
                non_pair_files.append(subpath)

    logger.debug(f"Found {len(matching_subfolders)} matching subfolders.")
    logger.debug(f"Found {len(pair_files)} pair files.")
    logger.debug(f"Found {len(non_pair_files)} non-pair files.")

    max_length = max(len(matching_subfolders), len(pair_files), len(non_pair_files))
    if len(matching_subfolders) == max_length:
        logger.info("Treating it as a folder of datasets.")
        return MultipleFolderImporter(matching_subfolders)
    elif len(pair_files) == max_length:
        logger.info("Treating it as files of pairs.")
        return MultipleFilesImporter(pair_files)
    elif len(non_pair_files) == max_length:
        logger.info("Treating it as files of non-pairs.")
        return MultipleFilesImporter(non_pair_files)
    else:
        raise ValueError("The folder structure is not consistent.")

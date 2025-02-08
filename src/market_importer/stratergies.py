from abc import ABC, abstractmethod
from typing import Generator

import pandas as pd

from runtime.enums import is_valid_currency_pair

DataframeSlot = str


def is_forex(literal: str, dictionary: set[str]) -> bool:
    """
    Check if a folder name can be split into exactly two valid 3-char words from the dictionary.
    """
    literal = literal.upper()
    if len(literal) != 6:
        return False
    return literal[:3] in dictionary and literal[3:] in dictionary


class ImportStrategy(ABC):
    def __init__(self, base):
        self.base = base

    @abstractmethod
    def collect_iter(self) -> Generator[DataframeSlot, None, None]:
        pass


class MultipleFolderImporter(ImportStrategy):
    """Imports datasets from a list of folders."""

    def collect_iter(self) -> Generator[DataframeSlot, None, None]:
        pass


class MultipleFilesImporter(ImportStrategy):
    """Import datasets from a list of files."""

    def __init__(self, base):
        super().__init__(base)

    def collect_iter(self) -> Generator[DataframeSlot, None, None]:
        for file in self.base:
            file_import = SingleFileImporter(file)
            for dfs in file_import.collect_iter():
                yield dfs


class SingleFileImporter(ImportStrategy):
    """Import a single file."""

    def collect_iter(self) -> Generator[DataframeSlot, None, None]:
        symbol = self.base.stem.upper()

        metadata = {"symbol": symbol}

        if is_valid_currency_pair(symbol):
            metadata["forex_base"] = (symbol[:3],)
            metadata["forex_counter"] = symbol[3:]
            metadata["family"] = "forex"

        if self.base.suffix == ".parquet":
            df = pd.read_parquet(self.base)
            yield DataframeSlot(df, metadata)


class FolderImporter(ImportStrategy):
    pass


class CompressedImporter(ImportStrategy):
    pass

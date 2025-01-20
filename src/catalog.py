"""Holds the library definition"""

from market_importer.market_importer import MarketImporter
from runtime.catalog_base import Catalog


catalog = Catalog(
    name="python-operator-catalog",
    description="Basic operators written in Python",
    operators=[
        MarketImporter,
    ],
)

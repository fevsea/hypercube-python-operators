"""Holds the library definition"""

from market_importer.market_importer import MarketImporter
from runtime.catalog_base import Catalog


catalog = Catalog(
    name="python-component-catalog",
    description="Basic components written in Python",
    components=[
        MarketImporter,
    ],
)

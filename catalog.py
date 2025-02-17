"""Holds the library definition"""

from hypercube.runtime.catalog_base import Catalog

from market_importer.market_importer import market_importer

catalog = Catalog(
    name="python-component-catalog",
    description="Basic components written in Python",
    components=[
        market_importer.component,
    ],
)

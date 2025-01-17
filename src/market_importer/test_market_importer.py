from market_importer.market_importer import MarketImporter


def test_success():
    """Pytest success"""
    assert True


def test_instantiation():
    """Test instantiation of market_importer"""
    options = MarketImporter.Options()

    market_importer = MarketImporter(tuple(), options)
    assert market_importer is not None

from src.energy_ml import __version__


def test_version():
    assert isinstance(__version__, str) and len(__version__) > 0

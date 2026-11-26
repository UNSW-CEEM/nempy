"""Pytest configuration shared by the whole repository.

Some tests and doctests reach out to nemweb to download AEMO data. They are
deselected unless ``--run-network`` is passed.

Run the default (offline) suite with::

    uv run pytest

Include the network-dependent tests and doctests with::

    uv run pytest --run-network
"""

from pathlib import Path

# Paths deselected unless --run-network is given. A directory covers everything
# beneath it, which is how the historical_inputs doctests are excluded.
_NETWORK_PATHS = (
    Path("src") / "nempy" / "historical_inputs",
    Path("tests") / "test_constraint_equation_calc.py",
    Path("tests") / "test_historical_spot_market_inputs.py",
)


def pytest_addoption(parser):
    parser.addoption(
        "--run-network",
        action="store_true",
        default=False,
        help=(
            "Run tests and doctests that download AEMO data from nemweb. Slow, and "
            "writes downloaded artefacts into the working directory."
        ),
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "network: test that downloads data from nemweb; deselected unless --run-network.",
    )


def pytest_ignore_collect(collection_path, config):
    """Skip collection of network-dependent paths unless --run-network is given."""
    if config.getoption("--run-network"):
        return None
    try:
        relative = Path(collection_path).relative_to(Path(config.rootpath))
    except ValueError:
        return None
    for network_path in _NETWORK_PATHS:
        if relative == network_path or network_path in relative.parents:
            return True
    return None

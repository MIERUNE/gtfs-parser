import os

import pytest

from gtfs_parser.gtfs import GTFSFactory


@pytest.fixture
def gtfs():
    FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixture")
    return GTFSFactory(FIXTURE_DIR)

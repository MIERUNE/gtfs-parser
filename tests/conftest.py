import os

import pytest

from gtfs_parser.gtfs import GTFS

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixture")


@pytest.fixture
def gtfs():
    return GTFS(FIXTURE_DIR)

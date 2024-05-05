import os

import pandas as pd


from gtfs_parser.gtfs import GTFSFactory


def test_gtfs():
    FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixture")
    gtfs = GTFSFactory(FIXTURE_DIR)

    # required tables
    assert isinstance(gtfs.agency, pd.DataFrame)
    assert isinstance(gtfs.routes, pd.DataFrame)
    assert isinstance(gtfs.stop_times, pd.DataFrame)
    assert isinstance(gtfs.stops, pd.DataFrame)
    assert isinstance(gtfs.trips, pd.DataFrame)

    # optional but in fixture
    assert isinstance(gtfs.feed_info, pd.DataFrame)
    assert isinstance(gtfs.shapes, pd.DataFrame)
    assert isinstance(gtfs.calendar, pd.DataFrame)
    assert isinstance(gtfs.calendar_dates, pd.DataFrame)

import os

import pandas as pd


from gtfs_parser.gtfs import GTFSFactory


def test_gtfs():
    FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixture")
    gtfs = GTFSFactory(FIXTURE_DIR)

    # required tables
    assert isinstance(gtfs.agency, pd.DataFrame)
    assert isinstance(gtfs.calendar, pd.DataFrame)
    assert isinstance(gtfs.routes, pd.DataFrame)
    assert isinstance(gtfs.stop_times, pd.DataFrame)
    assert isinstance(gtfs.stops, pd.DataFrame)
    assert isinstance(gtfs.trips, pd.DataFrame)

    # optional but in fixture
    assert isinstance(gtfs.fare_attributes, pd.DataFrame)
    assert isinstance(gtfs.fare_rules, pd.DataFrame)
    assert isinstance(gtfs.feed_info, pd.DataFrame)
    assert isinstance(gtfs.office_jp, pd.DataFrame)
    assert isinstance(gtfs.shapes, pd.DataFrame)
    assert isinstance(gtfs.translations, pd.DataFrame)
    assert isinstance(gtfs.calendar_dates, pd.DataFrame)

    # optional but not in fixture
    assert gtfs.agency_jp is None
    assert gtfs.frequencies is None
    assert gtfs.routes_jp is None
    assert gtfs.transfers is None

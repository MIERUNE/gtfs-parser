import os
import glob


from gtfs_parser.gtfs import GTFS


def test_gtfs():
    FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixture")
    gtfs = GTFS(FIXTURE_DIR)
    # 13 txt files are in ./fixture
    assert 13 == len(glob.glob(os.path.join(FIXTURE_DIR, "*.txt")))
    # read tables in constants.py
    assert 13 == len(gtfs.keys())

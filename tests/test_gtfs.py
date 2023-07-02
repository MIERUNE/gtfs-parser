import os
import unittest
import glob

from gtfs_parser.gtfs import GTFS

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixture")


class TestGtfs(unittest.TestCase):
    gtfs = GTFS(FIXTURE_DIR)

    def test_gtfs(self):
        # 13 txt files are in ./fixture
        self.assertEqual(13, len(glob.glob(os.path.join(FIXTURE_DIR, "*.txt"))))
        # read tables in constants.py
        self.assertEqual(13, len(self.gtfs.keys()))

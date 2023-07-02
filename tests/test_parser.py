import os
import unittest

from gtfs_parser.gtfs import GTFS
from gtfs_parser.parse import read_routes, read_stops

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixture")


class TestParser(unittest.TestCase):
    gtfs = GTFS(FIXTURE_DIR)

    def test_read_stops(self):
        stops_features = read_stops(self.gtfs)
        # list of geojson-feature
        self.assertEqual(899, len(stops_features))

        # remove no-route stops
        stops_features_no_noroute = read_stops(self.gtfs, ignore_no_route=True)
        self.assertEqual(896, len(stops_features_no_noroute))

    def test_read_routes(self):
        routes_features = read_routes(self.gtfs)
        self.assertEqual(32, len(routes_features))

        # num of features in routes.geojson depends on not shapes.txt but routes.txt
        routes_features_noshapes = read_routes(self.gtfs, ignore_shapes=True)
        self.assertEqual(32, len(routes_features_noshapes))

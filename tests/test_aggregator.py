import os
import unittest

from gtfs_parser.gtfs import GTFS
from gtfs_parser.aggregate import Aggregator

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixture")


class TestAggregator(unittest.TestCase):
    gtfs = GTFS(FIXTURE_DIR)

    def test_read_interpolated_stops(self):
        aggregator = Aggregator(self.gtfs)
        interpolated_stops_features = aggregator.read_interpolated_stops()

        # as_unify means near and similar named stops move into same lat-lon(centroid of them)
        self.assertEqual(518, len(interpolated_stops_features))

        # read_interpolated_stops unify stops having same lat-lon into one featrure.
        # there are no stops having same lat-lon in fixture
        aggregator_nounify = Aggregator(self.gtfs, no_unify_stops=True)
        nounify_features = aggregator_nounify.read_interpolated_stops()
        self.assertEqual(899, len(nounify_features))

    def test_read_route_frequency(self):
        # unify some 'similar' stops into same position, this decrease num of route_frequency features
        aggregator = Aggregator(self.gtfs)
        self.assertEqual(918, len(aggregator.read_route_frequency()))

        # each route_frequency feature is drawn between 2 stops
        aggregator_nounify = Aggregator(self.gtfs, no_unify_stops=True)
        self.assertEqual(956, len(aggregator_nounify.read_route_frequency()))

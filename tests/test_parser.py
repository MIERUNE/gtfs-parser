from gtfs_parser.parse import read_routes, read_stops


def test_read_stops(gtfs):
    stops_features = read_stops(gtfs)
    # list of geojson-feature
    assert 899 == len(stops_features)


def test_read_stops_ignore_no_route(gtfs):
    # remove no-route stops
    stops_features_no_noroute = read_stops(gtfs, ignore_no_route=True)
    assert 896 == len(stops_features_no_noroute)


def test_read_routes(gtfs):
    routes_features = read_routes(gtfs)
    assert 32 == len(routes_features)


def test_read_routes_ignore_shapes(gtfs):
    # num of features in routes.geojson depends on not shapes.txt but routes.txt
    routes_features_noshapes = read_routes(gtfs, ignore_shapes=True)
    assert 32 == len(routes_features_noshapes)

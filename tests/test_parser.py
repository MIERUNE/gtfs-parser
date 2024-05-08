from gtfs_parser.parse import read_routes, read_stops


def __find_feature_by(properties, features):
    for feature in features:
        if all(
            feature["properties"].get(key) == value for key, value in properties.items()
        ):
            return feature
    return None


def test_read_stops(gtfs):
    stops_features = read_stops(gtfs)
    # list of geojson-feature
    assert 899 == len(stops_features)

    # this feature exists in stops.geojson
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [143.203227, 42.918326]},
        "properties": {
            "stop_id": "1000_04",
            "stop_name": "帯広駅バスターミナル",
            "route_ids": [
                "24_C",
                "53_F",
                "32_B",
                "33_C",
                "51_C",
                "34_A",
                "42_B",
                "41_A",
                "31_A",
                "52_D",
            ],
        },
    }

    found = __find_feature_by(feature["properties"], stops_features)
    assert found is not None


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

from gtfs_parser.aggregate import Aggregator


def is_coordinate_close(ref_coord, tgt_coord):
    e = 0.000001
    return ref_coord[0] - e <= tgt_coord[0] <= ref_coord[0] + e \
           and ref_coord[1] - e <= tgt_coord[1] <= ref_coord[1] + e


def is_geometry_close(ref, tgt):
    geom_type = ref["type"]
    if geom_type != tgt["type"]:
        return False
    elif geom_type == "Point":
        return is_coordinate_close(ref["coordinates"], tgt["coordinates"])
    elif geom_type == "LineString":
        return all(is_coordinate_close(r, t) for r, t in zip(ref["coordinates"], tgt["coordinates"]))
    elif geom_type == "MultiLineString":
        for line_ref, line_tgt in zip(ref["coordinates"], tgt["coordinates"]):
            if all(is_coordinate_close(r, t) for r, t in zip(line_ref, line_tgt)):
                return True
    return False


def feature_exists(reference, features):
    ref_properties = reference["properties"]
    for feature in features:
        tgt_property = feature["properties"]
        if all(tgt_property.get(key) == value for key, value in ref_properties.items()):
            if "geometry" in reference:
                return is_geometry_close(reference.get("geometry"), feature["geometry"])
            else:
                return True
    return False


def properties_exists(reference, properties):
    for tgt_property in properties:
        if all(tgt_property.get(key) == value for key, value in reference.items()):
            return True
    return False


def test_no_unify(gtfs):
    aggregator = Aggregator(gtfs, yyyymmdd="20210721", no_unify_stops=True)

    # The number of stops is reduced by the parent stop, rather than the number on stops.txt
    relations = aggregator.read_stop_relations()
    assert 896 == len(relations)

    stop_features = aggregator.read_interpolated_stops()
    assert 896 == len(stop_features)

    route_features = aggregator.read_route_frequency()
    assert 954 == len(route_features)

    assert feature_exists({
        "properties" : {
            "frequency": 33,
            "prev_stop_id": "1000_02",
            "prev_stop_name": "帯広駅バスターミナル",
            "next_stop_id": "1001_01",
            "next_stop_name": "駅北11丁目",
            "agency_id": "8460101001629",
            "agency_name": "北海道拓殖バス株式会社",
        },
        "geometry": {
            "type": "LineString",
            "coordinates": [(143.203641761905, 42.9181203333334), (143.202304, 42.919629)]
        }
    }, route_features)


def test_unify_no_delimiter(gtfs):
    aggregator = Aggregator(gtfs, yyyymmdd="20210721")

    relations = aggregator.read_stop_relations()
    assert 896 == len(relations)

    # They are close and have same name, but have different id prefix.
    assert properties_exists({
        "stop_id": "9157_01",
        "stop_name": "緑陽台公園前",
        "similar_stop_id": "2309_01",
        "similar_stop_name": "緑陽台公園前"
    }, relations)

    # This stop has same name stops, but they have different id prefix and are far from it.
    assert properties_exists({
        "stop_id": "9159_01",
        "stop_name": "緑陽台南区",
        "similar_stop_id": "9159_01",
        "similar_stop_name": "緑陽台南区"
    }, relations)

    # unify some 'similar' stops into same position,
    # this decrease num of read_interpolated_stops, route_frequency features.
    stop_features = aggregator.read_interpolated_stops()
    assert 519 == len(stop_features)

    assert properties_exists({
        "properties": {
            "similar_stop_id": "2309_01",
            "similar_stop_name": "緑陽台公園前",
            "count": 60,
        },
        "geometry": {
            "type": "Point",
            "coordinates": [143.19664133361098, 42.96935276793033]
        }
    }, stop_features)

    route_features = aggregator.read_route_frequency()
    assert 916 == len(route_features)

    assert feature_exists({
        "properties" : {
            "frequency": 171,
            "prev_stop_id": "1002_02",
            "prev_stop_name": "かじのビル前",
            "next_stop_id": "1000_親",
            "next_stop_name": "帯広駅バスターミナル",
            "agency_id": "8460101001629",
            "agency_name": "北海道拓殖バス株式会社",
        },
        "geometry": {
            "type": "LineString",
            "coordinates": [(143.202401000868, 42.9208138405135), (143.203424111165, 42.9183318330811)]
        }
    }, route_features)


def test_unify_delimiter(gtfs):
    aggregator = Aggregator(gtfs, yyyymmdd="20210721", delimiter="_")

    relations = aggregator.read_stop_relations()
    assert 896 == len(relations)

    # They are close and have same name, but have different id prefix.
    assert properties_exists({
        "stop_id": "9157_01",
        "stop_name": "緑陽台公園前",
        "similar_stop_id": "9157",
        "similar_stop_name": "緑陽台公園前"
    }, relations)

    # Compared to the case of no_delimiter, the number of stops is increased
    # by the number of stops that have same names but different id prefixes.
    stop_features = aggregator.read_interpolated_stops()
    assert 559 == len(stop_features)

    assert properties_exists({
        "properties": {
            "similar_stop_id": "2309",
            "similar_stop_name": "緑陽台公園前",
            "count": 57,
        },
        "geometry": {
            "type": "Point",
            "coordinates": [143.1966675, 42.9692865]
        }
    }, stop_features)

    route_features = aggregator.read_route_frequency()
    assert 931 == len(route_features)

    assert feature_exists({
        "properties" : {
            "frequency": 171,
            "prev_stop_id": "1002",
            "prev_stop_name": "かじのビル前",
            "next_stop_id": "1000_親",
            "next_stop_name": "帯広駅バスターミナル",
            "agency_id": "8460101001629",
            "agency_name": "北海道拓殖バス株式会社",
        },
        "geometry": {
            "type": "LineString",
            "coordinates": [(143.202401000868, 42.9208138405135), (143.203424111165, 42.9183318330811)]
        }
    }, route_features)


def test_unify_delimiter_night(gtfs):
    # Specifying time reduces the number of route_frequency.
    aggregator = Aggregator(gtfs, yyyymmdd="20210721", delimiter="_", begin_time="200000", end_time="270000")

    stop_features = aggregator.read_interpolated_stops()
    assert 559 == len(stop_features)

    relations = aggregator.read_stop_relations()
    assert 896 == len(relations)

    route_features = aggregator.read_route_frequency()
    assert 293 == len(route_features)

    assert feature_exists({
        "properties" : {
            "frequency": 11,
            "prev_stop_id": "1002",
            "prev_stop_name": "かじのビル前",
            "next_stop_id": "1000_親",
            "next_stop_name": "帯広駅バスターミナル",
            "agency_id": "8460101001629",
            "agency_name": "北海道拓殖バス株式会社",
        },
        "geometry": {
            "type": "LineString",
            "coordinates": [(143.202401000868, 42.9208138405135), (143.203424111165, 42.9183318330811)]
        }
    }, route_features)


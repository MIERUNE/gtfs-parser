import pandas as pd

from .gtfs import GTFS


def read_stops(gtfs: GTFS, ignore_no_route=False) -> list:
    """
    read stops by stops table

    Args:
        ignore_no_route (bool, optional): stops unconnected to routes are skipped. Defaults to False.

    Returns:
        list: [description]
    """
    # get unique list of route_id related to each stop
    stop_trip_route_df = pd.merge(
        gtfs.stop_times[["trip_id", "stop_id"]],
        gtfs.trips[["trip_id", "route_id"]],
        on="trip_id",
    )
    stop_route_df = stop_trip_route_df[["stop_id", "route_id"]].drop_duplicates()
    route_ids_on_stops = (
        stop_route_df.groupby("stop_id")["route_id"].apply(list).rename("route_ids")
    )
    # outer join route_ids to stop
    route_stop = gtfs.stops.join(route_ids_on_stops, on="stop_id", how="left")

    if ignore_no_route:
        # remove stops unconnected to routes
        route_stop = route_stop[~route_stop["route_ids"].isna()]
    else:
        # fill na with empty list
        route_stop["route_ids"] = route_stop["route_ids"].fillna("").apply(list)

    # parse stops to GeoJSON-Features
    stop_dics = route_stop.to_dict(orient="records")
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["stop_lon"], row["stop_lat"]],
            },
            "properties": {
                "stop_id": row["stop_id"],
                "stop_name": row["stop_name"],
                "route_ids": row["route_ids"],
            },
        }
        for row in stop_dics
    ]

    return features


def read_routes(gtfs: GTFS, ignore_shapes=False) -> list:
    """
    read routes by shapes or stop_times
    First, this method try to load shapes and parse it into routes,
    but shapes is optional table in GTFS. Then is shapes does not exist or no_shapes is True,
    this parse routes by stop_time, stops, trips, and routes.

    Args:
        no_shapes (bool, optional): ignore shapes table. Defaults to False.

    Returns:
        [list]: list of GeoJSON-Feature-dict
    """

    if gtfs.shapes is None or ignore_shapes:
        return __read_routes_ignore_shapes(gtfs)
    else:
        return __read_route_shapes(gtfs)


def __read_route_shapes(gtfs):
    # get_shapeids_on route
    shape_ids_on_routes = (
        gtfs.trips[["route_id", "shape_id"]]
        .drop_duplicates()
        .dropna(subset=["shape_id"])
        .sort_values(["route_id", "shape_id"])
    )

    # get shape coordinate
    shapes_df = gtfs.shapes.copy()
    shapes_df["shape_pt"] = list(
        zip(shapes_df["shape_pt_lon"], shapes_df["shape_pt_lat"])
    )
    shapes_df = shapes_df.sort_values(["shape_id", "shape_pt_sequence"])
    shape_lines = (
        shapes_df.groupby("shape_id")["shape_pt"]
        .apply(lambda x: x.tolist())
        .rename("line")
    )

    # merge
    route_line_df = pd.merge(shape_ids_on_routes, shape_lines, on="shape_id")
    route_lines = route_line_df.set_index("route_id")["line"]

    features = __route_lines_to_features(route_lines, gtfs.routes)

    # load shapes unloaded yet
    unloaded_shape_lines = shape_lines[
        ~shape_lines.index.isin(shape_ids_on_routes["shape_id"].unique())
    ]
    if len(unloaded_shape_lines) > 0:
        # fill id, name with shape_id, line to multiline
        multiline_df = pd.DataFrame(
            {
                "route_id": None,
                "route_name": unloaded_shape_lines.index,
                "multiline": unloaded_shape_lines.apply(lambda x: [x]),
            }
        )

        unloaded_features = __route_multiline_df_to_features(multiline_df)
        features.extend(unloaded_features)
    return features


def __read_routes_ignore_shapes(gtfs):
    # generate stop patterns
    sorted_stop_times = gtfs.stop_times.sort_values(["trip_id", "stop_sequence"])
    trip_stop_pattern = (
        sorted_stop_times.groupby("trip_id")["stop_id"]
        .agg(tuple)
        .rename("stop_pattern")
    )

    # unique stop pattens by route_id
    route_trip_stop_pattern = pd.merge(
        trip_stop_pattern, gtfs.trips[["trip_id", "route_id"]], on="trip_id"
    )
    route_stop_patterns = route_trip_stop_pattern[
        ["route_id", "stop_pattern"]
    ].drop_duplicates()

    # explode stop patterns to stop ids
    route_stop_patterns["stop_id"] = route_stop_patterns["stop_pattern"]
    route_stop_ids = route_stop_patterns.explode("stop_id")

    # append geometry to stops
    stop_geoms = pd.Series(
        data=zip(gtfs.stops["stop_lon"], gtfs.stops["stop_lat"]),
        name="stop_pt",
        index=gtfs.stops["stop_id"],
    )

    # join geomtry to route stops
    route_stop_ids["order"] = range(len(route_stop_ids))
    route_stop_geoms = pd.merge(
        route_stop_ids,
        stop_geoms,
        on="stop_id",
    ).sort_values("order")

    # Point -> LineString: group by route_id and stop_pattern
    route_lines = route_stop_geoms.groupby(["route_id", "stop_pattern"])["stop_pt"].agg(
        list
    )
    return __route_lines_to_features(route_lines, gtfs.routes)


def __route_lines_to_features(route_lines, routes):
    # group by route_id into MultiLineString
    multilines = (
        route_lines.groupby(["route_id"])
        .apply(lambda x: x.tolist())
        .rename("multiline")
    )
    # join route_id and route_name
    multiline_df = pd.merge(
        multilines,
        routes[["route_id", "route_long_name", "route_short_name"]],
        on="route_id",
    )
    multiline_df["route_name"] = multiline_df["route_long_name"].fillna(
        ""
    ) + multiline_df["route_short_name"].fillna("")

    return __route_multiline_df_to_features(multiline_df)


def __route_multiline_df_to_features(multiline_df):
    dicts = multiline_df.to_dict(orient="records")
    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "MultiLineString",
                "coordinates": row["multiline"],
            },
            "properties": {
                "route_id": row["route_id"],
                "route_name": row["route_name"],
            },
        }
        for row in dicts
    ]
    return features

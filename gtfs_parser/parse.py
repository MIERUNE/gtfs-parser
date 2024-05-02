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
    stop_times_trip_df = pd.merge(
        gtfs.stop_times,
        gtfs.trips,
        on="trip_id",
    )
    route_ids_on_stops = stop_times_trip_df.groupby("stop_id")["route_id"].unique()

    # join route_id to stop
    route_stop = pd.merge(gtfs.stops, route_ids_on_stops, on="stop_id", how="left")
    # rename column: route_id -> route_ids
    route_stop.rename(columns={"route_id": "route_ids"}, inplace=True)
    # fill na with empty list
    route_stop["route_ids"] = route_stop["route_ids"].fillna("").apply(list)

    if ignore_no_route:  # remove stops unconnected to routes
        route_stop = route_stop[route_stop["route_ids"].apply(len) > 0]

    # parse stops to GeoJSON-Features
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
        for _, row in route_stop.iterrows()
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
        # trip-route-merge:A
        trips_routes = pd.merge(
            gtfs.trips[["trip_id", "route_id"]],
            gtfs.routes[["route_id", "route_long_name", "route_short_name"]],
            on="route_id",
        )

        # stop_times-stops-merge:B
        stop_times_stop = pd.merge(
            gtfs.stop_times[["stop_id", "trip_id", "stop_sequence"]],
            gtfs.stops[["stop_id", "stop_lon", "stop_lat"]],
            on="stop_id",
        )

        stop_times_stop["stop_pt"] = stop_times_stop.apply(
            lambda x: (x["stop_lon"], x["stop_lat"]), axis=1
        )

        # A-B-merge
        merged = pd.merge(stop_times_stop, trips_routes, on="trip_id")
        # sort by route_id, trip_id, stop_sequence
        merged.sort_values(["route_id", "trip_id", "stop_sequence"], inplace=True)

        # Point -> LineString: group by route_id and trip_id
        lines = merged.groupby(["route_id", "trip_id"])["stop_pt"].apply(
            lambda x: x.tolist()
        )
        lines = lines.drop_duplicates()
        line_df = lines.reset_index()
        # rename: stop_pt -> line
        line_df.rename(columns={"stop_pt": "line"}, inplace=True)

        # group by route_id into MultiLineString
        multilines = line_df.groupby(["route_id"])["line"].apply(lambda x: x.to_list())
        multiline_df = multilines.reset_index()

        # join route_id and route_name
        multiline_df = pd.merge(
            multiline_df[["route_id", "line"]],
            gtfs.routes[["route_id", "route_long_name", "route_short_name"]],
            on="route_id",
        )
        multiline_df["route_name"] = multiline_df["route_long_name"].fillna(
            ""
        ) + multiline_df["route_short_name"].fillna("")

        # to GeoJSON-Feature
        features = [
            {
                "type": "Feature",
                "geometry": {
                    "type": "MultiLineString",
                    "coordinates": row["line"],
                },
                "properties": {
                    "route_id": row["route_id"],
                    "route_name": row["route_name"],
                },
            }
            for _, row in multiline_df.iterrows()
        ]
        return features
    else:
        # get_shapeids_on route
        trips_with_shape_df = gtfs.trips[["route_id", "shape_id"]].dropna(
            subset=["shape_id"]
        )
        shape_ids_on_routes = trips_with_shape_df.groupby("route_id")[
            "shape_id"
        ].unique()
        shape_ids_on_routes.apply(lambda x: x.sort())
        shape_ids_on_routes = shape_ids_on_routes.reset_index()
        shape_ids_on_routes = shape_ids_on_routes.explode("shape_id")

        # get shape coordinate
        shapes_df = gtfs.shapes.copy()
        shapes_df["shape_pt"] = shapes_df.apply(
            lambda x: (x["shape_pt_lon"], x["shape_pt_lat"]), axis=1
        )
        shapes_df.sort_values(["shape_id", "shape_pt_sequence"])
        lines = shapes_df.groupby("shape_id")["shape_pt"].apply(lambda x: x.tolist())
        line_df = lines.reset_index()
        line_df.rename(columns={"shape_pt": "shape_line"}, inplace=True)

        # merge
        multiline_df = pd.merge(shape_ids_on_routes, line_df, on="shape_id")
        # group by route_id into MultiLineString
        multiline_df = multiline_df.groupby(["route_id"])["shape_line"].apply(
            lambda x: x.to_list()
        )
        multiline_df = multiline_df.reset_index()

        # join routes
        multiline_df = pd.merge(
            multiline_df,
            gtfs.routes[["route_id", "route_long_name", "route_short_name"]],
            on="route_id",
        )

        # join route_names
        multiline_df["route_name"] = multiline_df["route_long_name"].fillna(
            ""
        ) + multiline_df["route_short_name"].fillna("")

        # to GeoJSON-Feature
        features = [
            {
                "type": "Feature",
                "geometry": {
                    "type": "MultiLineString",
                    "coordinates": row["shape_line"],
                },
                "properties": {
                    "route_id": row["route_id"],
                    "route_name": row["route_name"],
                },
            }
            for _, row in multiline_df.iterrows()
        ]

        # load shapes unloaded yet
        unloaded_shapes = gtfs.shapes[
            ~gtfs.shapes["shape_id"].isin(shape_ids_on_routes["shape_id"].unique())
        ]

        if len(unloaded_shapes) > 0:
            unloaded_shapes["shape_pt"] = unloaded_shapes.apply(
                lambda x: (x["shape_pt_lon"], x["shape_pt_lat"]), axis=1
            )
            # group by shape_id into LineString
            unloaded_shapes = unloaded_shapes.groupby("shape_id")["shape_pt"].apply(
                lambda x: x.tolist()
            )
            unloaded_shapes = unloaded_shapes.reset_index()
            unloaded_shapes.rename(columns={"shape_pt": "shape_line"}, inplace=True)

            # group by route_id into MultiLineString
            unloaded_shapes = unloaded_shapes.groupby(["shape_id"])["shape_line"].apply(
                lambda x: x.to_list()
            )
            unloaded_shapes = unloaded_shapes.reset_index()

            # fill id, name with shape_id
            unloaded_shapes["route_id"] = None
            unloaded_shapes["route_name"] = unloaded_shapes["shape_id"]

            # to GeoJSON-Feature
            unloaded_features = [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "MultiLineString",
                        "coordinates": row["shape_line"],
                    },
                    "properties": {
                        "route_id": row["route_id"],
                        "route_name": row["route_name"],
                    },
                }
                for _, row in unloaded_shapes.iterrows()
            ]
            features.extend(unloaded_features)

        return features

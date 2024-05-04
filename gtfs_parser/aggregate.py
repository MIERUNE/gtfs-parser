import datetime

import pandas as pd

from .gtfs import GTFS


class Aggregator:
    """
    Read stops "interpolated" by parent station or stop_id or stop_name and distance.
    There are many similar stops that are near to each, has same name, or has same prefix in stop_id.
    In traffic analyzing, it is good for that similar stops to be grouped as same stop.
    This method group them by some elements, parent, id, name and distance.

    Args:
        delimiter (str, optional): stop_id delimiter, sample_A, sample_B, then delimiter is '_'. Defaults to ''.
        max_distance_degree (float, optional): distance limit in grouping by stop_name. Defaults to 0.003.(approx. 300m)

    Returns:
        [type]: [description]
    """
    def __init__(
        self,
        gtfs: GTFS,
        no_unify_stops=False,
        delimiter="",
        max_distance_degree=0.003,
        yyyymmdd="",
        begin_time="",
        end_time="",
    ):
        self.gtfs = gtfs

        self.stop_times = Aggregator.__filter_stop_times(self.gtfs, yyyymmdd, begin_time, end_time)

        if no_unify_stops:
            similar_results = Aggregator.__get_similar_stop_without_unifying(self.gtfs.stops)
        else:
            similar_results = Aggregator.__unify_similar_stops(self.gtfs.stops, delimiter, max_distance_degree)
        self.similar_stops, self.stop_relations = similar_results

    @staticmethod
    def __filter_stop_times(gtfs, yyyymmdd, begin_time, end_time):
        # filter stop_times by whether serviced or not
        if yyyymmdd:
            trip_ids_filtered_by_day = Aggregator.__get_trips_on_a_date(gtfs, yyyymmdd)
            filtered_stop_times = gtfs.stop_times[
                gtfs.stop_times["trip_id"].isin(trip_ids_filtered_by_day)
            ]
        else:
            filtered_stop_times = gtfs.stop_times.copy()
        # time filter
        if begin_time and end_time:
            # departure_time is nullable and expressed in "hh:mm:ss" or "h:mm:ss" format.
            # Hour can be mor than 24.
            # Therefore, drop null records and convert times to integers.
            filtered_stop_times = filtered_stop_times[~filtered_stop_times["departure_time"].isnull()]
            int_dep_times = filtered_stop_times.departure_time.str.replace(
                ":", ""
            ).astype(int)
            filtered_stop_times = filtered_stop_times[(int_dep_times >= int(begin_time))
                                                      & (int_dep_times < int(end_time))]
        return filtered_stop_times

    @staticmethod
    def __get_similar_stop_without_unifying(stops):
        if "location_type" in stops:
            stops = stops[stops["location_type"] == 0]
        similar_stops_centroid = stops[["stop_lon", "stop_lat"]].values.tolist()

        similar_stops = pd.DataFrame({
            "similar_stop_id": stops["stop_id"],
            "similar_stop_name": stops["stop_name"],
            "similar_stops_centroid": similar_stops_centroid,
        })
        similar_relations = pd.concat([
            stops["stop_id"],
            similar_stops["similar_stop_id"]
        ], axis=1)
        return similar_stops, similar_relations

    @staticmethod
    def __unify_similar_stops(stops, delimiter, max_distance_degree):
        child_similar_stop = None
        child_id_pair = None

        # unify by parent_station
        if "location_type" in stops.columns:
            child_similar_stop, child_id_pair = Aggregator.__unify_child_stops(stops)

            solo_stops = stops[
                ~stops["stop_id"].isin(child_id_pair["stop_id"])
                & (stops["location_type"] == 0)
            ]
        else:
            solo_stops = stops

        # unify solo stops
        solo_similar_stops, solo_id_pair = Aggregator.__unify_solo_stops(solo_stops, delimiter, max_distance_degree)

        # concat similar stops
        return pd.concat([child_similar_stop, solo_similar_stops]), pd.concat([child_id_pair, solo_id_pair])

    @staticmethod
    def __unify_child_stops(stops):
        child_id_pair = stops[
            stops["parent_station"].isin(stops["stop_id"])
            & (stops["location_type"] == 0)
        ][["stop_id", "parent_station"]]

        child_id_pair.rename(columns={"parent_station": "similar_stop_id"}, inplace=True)

        similar_ids = child_id_pair["similar_stop_id"].unique()

        similar_stops = stops[stops["stop_id"].isin(similar_ids)][["stop_id", "stop_name", "stop_lon", "stop_lat"]]

        similar_stops["similar_stops_centroid"] = similar_stops[
            ["stop_lon", "stop_lat"]
        ].values.tolist()
        similar_stops.drop(columns=["stop_lon", "stop_lat"], inplace=True)

        similar_stops.rename(columns={
            "stop_id": "similar_stop_id",
            "stop_name": "similar_stop_name",
        }, inplace=True)

        return similar_stops, child_id_pair

    @staticmethod
    def __unify_solo_stops(solo_stops, delimiter, max_distance_degree):
        delimited_id_pair = []
        if delimiter:
            # unify by delimiter
            stop_id_delimited = solo_stops["stop_id"].str.split(delimiter).str[0].rename("similar_stop_id")
            delimited_id_pair = pd.concat([solo_stops["stop_id"], stop_id_delimited], axis=1)[
                solo_stops["stop_id"] != stop_id_delimited
            ]
        if len(delimited_id_pair) == len(solo_stops):
            solo_id_pair = delimited_id_pair
        else:
            # unify by distance
            if len(delimited_id_pair) > 0:
                undelimited_stops = solo_stops[~solo_stops["stop_id"].isin(delimited_id_pair["stop_id"])]
            else:
                undelimited_stops = solo_stops
            near_id_pair = Aggregator.__calc_near_id_pair(undelimited_stops, max_distance_degree)

            if len(delimited_id_pair) == 0:
                solo_id_pair = near_id_pair
            else:
                solo_id_pair = pd.concat([delimited_id_pair, near_id_pair])

        if len(solo_id_pair) == 0:
            return None, None

        # calc similar stop attributes
        solo_stops_with_similar = pd.merge(solo_stops, solo_id_pair, on="stop_id")
        solo_similar_stops = solo_stops_with_similar.groupby("similar_stop_id").agg({
            'stop_name': 'min',
            'stop_lon': 'mean',
            'stop_lat': 'mean'
        }).reset_index()
        solo_similar_stops.rename(columns={"stop_name": "similar_stop_name"}, inplace=True)
        solo_similar_stops["similar_stops_centroid"] = solo_similar_stops[
            ["stop_lon", "stop_lat"]
        ].values.tolist()
        solo_similar_stops.drop(columns=["stop_lon", "stop_lat"], inplace=True)
        return solo_similar_stops, solo_id_pair

    @staticmethod
    def __calc_near_id_pair(solo_stops, max_distance_degree):
        stop_matrix = pd.merge(
            solo_stops,
            solo_stops,
            on="stop_name",
            suffixes=("", "_r")
        )
        near_matrix = stop_matrix[
            (stop_matrix["stop_lon"] - stop_matrix["stop_lon_r"]) ** 2
            + (stop_matrix["stop_lat"] - stop_matrix["stop_lat_r"]) ** 2
            <= max_distance_degree ** 2
        ]

        near_matrix = near_matrix[["stop_id", "stop_id_r"]]
        # The smallest stop id among the nearest stops is considered as the root id.
        near_id_pair = near_matrix.groupby("stop_id").min().reset_index()

        near_id_pair = Aggregator.__join_near_group(near_id_pair)
        return near_id_pair.rename(columns={"stop_id_r": "similar_stop_id"})

    """
    Join near groups of stops.
    Trace root stops up to 5 times and modify root id.
    """
    @staticmethod
    def __join_near_group(near_id_pair):
        for i in range(5):
            leaf_pair = near_id_pair.query("stop_id != stop_id_r")\
                .rename(columns={"stop_id": "stop_id_r", "stop_id_r": "stop_id_r2"})
            sub_pair = pd.merge(near_id_pair, leaf_pair, on="stop_id_r").drop(columns=["stop_id_r"])
            if len(sub_pair) == 0:
                break
            mod_id_trio = pd.merge(near_id_pair, sub_pair, on="stop_id", how="left")
            mod_id_trio.loc[~mod_id_trio['stop_id_r2'].isna(), 'stop_id_r'] = mod_id_trio['stop_id_r2']
            near_id_pair = mod_id_trio.drop(columns=["stop_id_r2"])

        return near_id_pair

    def read_interpolated_stops(self):
        stop_pass_count = self.stop_times.groupby("stop_id").size().rename("count")
        stop_pass_count = pd.merge(
            self.stop_relations,
            stop_pass_count,
            on="stop_id",
            how="left"
        )
        similar_pass_count = stop_pass_count.groupby("similar_stop_id").sum("count").astype(int)
        similar_stop_summary = self.similar_stops.merge(similar_pass_count,
                                                        on="similar_stop_id")

        stop_dicts = similar_stop_summary.to_dict(orient="records")

        return [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": stop["similar_stops_centroid"],
                },
                "properties": {
                    "similar_stop_name": stop["similar_stop_name"],
                    "similar_stop_id": stop["similar_stop_id"],
                    "count": stop["count"],
                },
            }
            for stop in stop_dicts
        ]

    def read_route_frequency(self):
        """
        By grouped stops, aggregate route frequency.
        Filtering trips by a date, you can aggregate frequency only route serviced on the date.

        Args:
            yyyymmdd (str, optional): date, like 20210401. Defaults to ''.
            begin_time (str, optional): 'hhmmss' <= departure time, like 030000. Defaults to ''.
            end_time (str, optional): 'hhmmss' > departure time, like 280000. Defaults to ''.

        Returns:
            [type]: [description]
        """
        #
        stop_times_df = pd.merge(
            self.stop_times[["trip_id", "stop_sequence", "stop_id"]],
            self.stop_relations,
            on="stop_id"
        )
        # append agency_id
        trip_agency_df = pd.merge(
            self.gtfs.trips[["trip_id", "route_id"]],
            self.gtfs.routes[["route_id", "agency_id"]],
            on="route_id"
        )
        stop_times_df = pd.merge(
            stop_times_df,
            trip_agency_df,
            on="trip_id"
        )
        stop_times_df = stop_times_df.sort_values(["trip_id", "stop_sequence"])

        # generate path by joining next stop_times
        stop_times_df["next_stop_id"] = stop_times_df["similar_stop_id"].shift(-1)
        stop_times_df["next_trip_id"] = stop_times_df["trip_id"].shift(-1)
        stop_times_df = stop_times_df[stop_times_df["trip_id"] == stop_times_df["next_trip_id"]]
        path_df = stop_times_df.rename(columns={"similar_stop_id": "prev_stop_id"})

        # count frequency
        path_freq_sr = path_df.groupby(["agency_id", "prev_stop_id", "next_stop_id"]).size()
        path_freq_sr.name = "frequency"
        path_freq_df = path_freq_sr.reset_index()

        # append path attributes
        for order in ["prev", "next"]:
            path_freq_df = pd.merge(
                path_freq_df,
                self.similar_stops,
                left_on=f"{order}_stop_id",
                right_on="similar_stop_id"
            )
            path_freq_df.rename(columns={
                "similar_stop_name": f"{order}_stop_name",
                "similar_stops_centroid": f"{order}_similar_stops_centroid"
            }, inplace=True)
            path_freq_df.drop(columns="similar_stop_id", inplace=True)

        path_freq_df = pd.merge(
            path_freq_df,
            self.gtfs.agency[["agency_id", "agency_name"]],
            on="agency_id"
        )
        # convert to features
        path_freq_dict = path_freq_df.to_dict(orient="records")
        return [
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": (
                        path["prev_similar_stops_centroid"],
                        path["next_similar_stops_centroid"],
                    ),
                },
                "properties": {
                    "frequency": path["frequency"],
                    "prev_stop_id": path["prev_stop_id"],
                    "prev_stop_name": path["prev_stop_name"],
                    "next_stop_id": path["next_stop_id"],
                    "next_stop_name": path["next_stop_name"],
                    "agency_id": path["agency_id"],
                    "agency_name": path["agency_name"],
                },
            }
            for path in path_freq_dict
        ]
    
    @staticmethod
    def __get_trips_on_a_date(gtfs, yyyymmdd: str):
        """
        get trips are on service on a date.

        Args:
            yyyymmdd (str): [description]

        Returns:
            [type]: [description]
        """
        # sunday, monday, tuesday...
        day_of_week = (
            datetime.date(int(yyyymmdd[0:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8]))
            .strftime("%A")
            .lower()
        )

        # filter services by calendar
        if gtfs.calendar is None:
            # generate an empty series if calendar.txt is missing because it is not required.
            service_ids_on = pd.Series(name="service_id", dtype=str)
        else:
            calendar = gtfs.calendar.astype(
                {"start_date": int, "end_date": int}
            )
            calendar = calendar[
                calendar[day_of_week] == "1"
            ]
            calendar = calendar.query(
                f"start_date <= {int(yyyymmdd)} and {int(yyyymmdd)} <= end_date",
                engine="python",
            )
            service_ids_on = calendar["service_id"]

        # filter services by dates
        if gtfs.calendar_dates is not None:
            filtered = gtfs.calendar_dates[
                gtfs.calendar_dates["date"] == yyyymmdd
            ][["service_id", "exception_type"]]
            to_be_removed_service_ids = filtered[filtered["exception_type"] == "2"][
                "service_id"
            ]
            to_be_appended_services_ids = filtered[filtered["exception_type"] == "1"][
                "service_id"
            ]

            service_ids_on = service_ids_on[
                ~service_ids_on.isin(to_be_removed_service_ids)
            ]
            service_ids_on = pd.concat([service_ids_on, to_be_appended_services_ids])

        # filter trips
        trips_in_services = gtfs.trips[
            gtfs.trips["service_id"].isin(service_ids_on)
        ]

        return trips_in_services["trip_id"]

    def read_stop_relations(self) -> list:
        stop_relation_df = pd.merge(
            self.stop_relations,
            self.gtfs.stops[["stop_id", "stop_name"]],
            on="stop_id"
        )
        stop_relation_df = pd.merge(
            stop_relation_df,
            self.similar_stops,
            on="similar_stop_id",
        )
        stop_relation_df = stop_relation_df.reindex(columns=["stop_id", "stop_name",
                                                             "similar_stop_id", "similar_stop_name"])
        return stop_relation_df.to_dict(orient="records")

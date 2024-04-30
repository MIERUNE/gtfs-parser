import glob
import os
import zipfile
import io
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import geopandas as gpd


def load_df(f: io.BufferedIOBase, table_name: str) -> pd.DataFrame | gpd.GeoDataFrame:
    df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values={""})
    if table_name == "shapes":
        df["shape_pt_lon"] = df["shape_pt_lon"].astype(float)
        df["shape_pt_lat"] = df["shape_pt_lat"].astype(float)
        df["geometry"] = gpd.points_from_xy(df["shape_pt_lon"], df["shape_pt_lat"])
        df = gpd.GeoDataFrame(df, geometry="geometry")
    elif table_name == "stops":
        df["stop_lon"] = df["stop_lon"].astype(float)
        df["stop_lat"] = df["stop_lat"].astype(float)
        df["geometry"] = gpd.points_from_xy(df["stop_lon"], df["stop_lat"])
        df = gpd.GeoDataFrame(df, geometry="geometry")
        if "parent_station" not in df:
            df["parent_station"] = None
    elif table_name == "stop_times":
        df["stop_sequence"] = df["stop_sequence"].astype(int)

    return df


@dataclass
class GTFS:
    """
    reference: https://www.mlit.go.jp/common/001283244.pdf
    """

    # standard
    agency: pd.DataFrame
    routes: pd.DataFrame
    stop_times: pd.DataFrame
    stops: gpd.GeoDataFrame
    trips: pd.DataFrame
    calendar: Optional[pd.DataFrame] = None
    calendar_dates: Optional[pd.DataFrame] = None
    fare_attributes: Optional[pd.DataFrame] = None
    fare_rules: Optional[pd.DataFrame] = None
    feed_info: Optional[pd.DataFrame] = None
    frequencies: Optional[pd.DataFrame] = None
    shapes: Optional[gpd.GeoDataFrame] = None
    transfers: Optional[pd.DataFrame] = None
    translations: Optional[pd.DataFrame] = None

    # JP
    routes_jp: Optional[pd.DataFrame] = None
    agency_jp: Optional[pd.DataFrame] = None
    office_jp: Optional[pd.DataFrame] = None


def GTFSFactory(gtfs_path: str) -> GTFS:
    """
    read GTFS file to memory.

    Args:
        path of zip file or directory containing txt files.
    Returns:
        GTFS: dataclass of GTFS tables.
    """
    tables = {}
    path = os.path.join(gtfs_path)
    if os.path.isdir(path):
        table_files = glob.glob(os.path.join(gtfs_path, "*.txt"))
        for table_file in table_files:
            table_name = os.path.splitext(os.path.basename(table_file))[0]
            with open(table_file, encoding="utf-8_sig") as f:
                tables[table_name] = load_df(f, table_name)
    else:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"zip file not found. ({path})")
        with zipfile.ZipFile(path) as z:
            for file_name in z.namelist():
                if (
                    file_name.endswith(".txt")
                    and os.path.basename(file_name) == file_name
                ):
                    table_name = os.path.splitext(os.path.basename(file_name))[0]
                    with z.open(file_name) as f:
                        tables[table_name] = load_df(f, table_name)

    # set agency_id when there is a single agency
    if len(tables["agency"]) == 1:
        if "agency_id" not in tables["agency"].columns or pd.isnull(
            tables["agency"]["agency_id"].iloc[0]
        ):
            tables["agency"]["agency_id"] = ""
        # set agency_id to routes
        tables["routes"]["agency_id"] = tables["agency"]["agency_id"].iloc[0]

    # if there are missing tables, exception is raised.
    gtfs = GTFS(
        agency=tables.get("agency"),
        calendar=tables.get("calendar"),
        routes=tables.get("routes"),
        stop_times=tables.get("stop_times"),
        stops=tables.get("stops"),
        trips=tables.get("trips"),
        calendar_dates=tables.get("calendar_dates"),
        fare_attributes=tables.get("fare_attributes"),
        fare_rules=tables.get("fare_rules"),
        feed_info=tables.get("feed_info"),
        frequencies=tables.get("frequencies"),
        shapes=tables.get("shapes"),
        transfers=tables.get("transfers"),
        routes_jp=tables.get("routes_jp"),
        agency_jp=tables.get("agency_jp"),
        office_jp=tables.get("office_jp"),
        translations=tables.get("translations"),
    )

    return gtfs

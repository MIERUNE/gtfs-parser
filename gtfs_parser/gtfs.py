import glob
import os
import zipfile
import io
from dataclasses import dataclass, fields
from typing import Optional

import pandas as pd


def load_df(f: io.BufferedIOBase, table_name: str) -> pd.DataFrame:
    df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values={""})
    if table_name == "shapes":
        df["shape_pt_lon"] = df["shape_pt_lon"].astype(float)
        df["shape_pt_lat"] = df["shape_pt_lat"].astype(float)
        df["shape_pt_sequence"] = df["shape_pt_sequence"].astype(int)
    elif table_name == "stops":
        df["stop_lon"] = df["stop_lon"].astype(float)
        df["stop_lat"] = df["stop_lat"].astype(float)
        if "parent_station" not in df:
            df["parent_station"] = None
        if "location_type" in df:
            df["location_type"] = df["location_type"].fillna("0").astype(int)
    elif table_name == "stop_times":
        df["stop_sequence"] = df["stop_sequence"].astype(int)

    return df


@dataclass
class GTFS:
    """
    reference: https://gtfs.org/schedule/reference/
    reference for Japan: https://www.mlit.go.jp/common/001283244.pdf
    """
    agency: pd.DataFrame
    routes: pd.DataFrame
    stop_times: pd.DataFrame
    stops: pd.DataFrame
    trips: pd.DataFrame
    calendar: Optional[pd.DataFrame] = None
    calendar_dates: Optional[pd.DataFrame] = None
    feed_info: Optional[pd.DataFrame] = None
    shapes: Optional[pd.DataFrame] = None


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
    used_tables = {field.name for field in fields(GTFS)}
    if os.path.isdir(path):
        table_files = glob.glob(os.path.join(gtfs_path, "*.txt"))
        for table_file in table_files:
            table_name = os.path.splitext(os.path.basename(table_file))[0]
            if table_name in used_tables:
                with open(table_file, encoding="utf-8_sig") as f:
                    tables[table_name] = load_df(f, table_name)

    elif os.path.isfile(path):
        with zipfile.ZipFile(path) as z:
            for file_name in z.namelist():
                if (
                    file_name.endswith(".txt")
                    and os.path.basename(file_name) == file_name
                ):
                    table_name = os.path.splitext(os.path.basename(file_name))[0]
                    if table_name in used_tables:
                        with z.open(file_name) as f:
                            tables[table_name] = load_df(f, table_name)
    else:
        raise FileNotFoundError(f"zip file not found. ({path})")

    if len(tables) == 0:
        raise FileNotFoundError(
            "txt files must be in the root level directory, not in a sub folder."
        )

    # set agency_id when there is a single agency
    if len(tables["agency"]) == 1:
        if "agency_id" not in tables["agency"].columns or pd.isnull(
            tables["agency"]["agency_id"].iloc[0]
        ):
            # fill agency_id with empty str when it is missing or null
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
        feed_info=tables.get("feed_info"),
        shapes=tables.get("shapes"),
    )

    return gtfs

import glob
import os
import zipfile
import pandas as pd
import io
from dataclasses import dataclass
from typing import Optional


def append_table(f: io.BufferedIOBase, table_path: str, table_dfs: dict):
    datatype = os.path.splitext(os.path.basename(table_path))[0]
    df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values={""})
    table_dfs[datatype] = df


@dataclass
class GTFS:
    """
    reference: https://www.mlit.go.jp/common/001283244.pdf
    """

    # standard
    agency: pd.DataFrame
    routes: pd.DataFrame
    stop_times: pd.DataFrame
    stops: pd.DataFrame
    trips: pd.DataFrame
    calendar: Optional[pd.DataFrame] = None
    calendar_dates: Optional[pd.DataFrame] = None
    fare_attributes: Optional[pd.DataFrame] = None
    fare_rules: Optional[pd.DataFrame] = None
    feed_info: Optional[pd.DataFrame] = None
    frequencies: Optional[pd.DataFrame] = None
    shapes: Optional[pd.DataFrame] = None
    transfers: Optional[pd.DataFrame] = None

    # JP
    routes_jp: Optional[pd.DataFrame] = None
    agency_jp: Optional[pd.DataFrame] = None
    office_jp: Optional[pd.DataFrame] = None

    # other
    translations: Optional[pd.DataFrame] = None


def GTFSFactory(gtfs_path: str) -> GTFS:
    """
    read GTFS file to memory.

    Args:
        path of zip file or directory containing txt files.
    Returns:
        dict: tables
    """
    tables = {}
    path = os.path.join(gtfs_path)
    if os.path.isdir(path):
        table_files = glob.glob(os.path.join(gtfs_path, "*.txt"))
        for table_file in table_files:
            with open(table_file, encoding="utf-8_sig") as f:
                append_table(f, table_file, tables)
    else:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"zip file not found. ({path})")
        with zipfile.ZipFile(path) as z:
            for file_name in z.namelist():
                if (
                    file_name.endswith(".txt")
                    and os.path.basename(file_name) == file_name
                ):
                    with z.open(file_name) as f:
                        append_table(f, file_name, tables)

    # check files.
    if len(tables) == 0:
        raise FileNotFoundError(
            "txt files must reside at the root level directly, not in a sub folder."
        )

    # cast some columns
    cast_columns = {
        "stops": {"stop_lon": float, "stop_lat": float},
        "stop_times": {"stop_sequence": int},
        "shapes": {
            "shape_pt_lon": float,
            "shape_pt_lat": float,
            "shape_pt_sequence": int,
        },
    }
    for table, casts in cast_columns.items():
        if table in tables:
            tables[table] = tables[table].astype(casts)

    # Set null values on optional columns used in this module.
    if "parent_station" not in tables["stops"].columns:
        tables["stops"]["parent_station"] = None

    # set agency_id when there is a single agency
    agency_df = tables["agency"]
    if len(agency_df) == 1:
        if "agency_id" not in agency_df.columns or pd.isnull(
            agency_df["agency_id"].iloc[0]
        ):
            agency_df["agency_id"] = ""
        agency_id = agency_df["agency_id"].iloc[0]
        tables["routes"]["agency_id"] = agency_id

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

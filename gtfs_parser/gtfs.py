import glob
import os
import zipfile
import pandas as pd
import io


def append_table(f: io.BufferedIOBase, table_path: str, table_dfs: dict):
    datatype = os.path.splitext(os.path.basename(table_path))[0]
    df = pd.read_csv(f, dtype=str)
    table_dfs[datatype] = df


def GTFS(gtfs_dir: str) -> dict:
    tables = {}
    path = os.path.join(gtfs_dir)
    if os.path.isdir(path):
        table_files = glob.glob(os.path.join(gtfs_dir, "*.txt"))
        for table_file in table_files:
            with open(table_file, encoding="utf-8_sig") as f:
                append_table(f, table_file, tables)
    else:
        if not os.path.isfile(path):
            print(f"zip file not found. ({path})")
            return None
        try:
            with zipfile.ZipFile(path) as z:
                for file_name in z.namelist():
                    if file_name.endswith(".txt"):
                        with z.open(file_name) as f:
                            append_table(f, file_name, tables)
        except Exception as e:
            print(f"zip file read error. ({path}: {str(e)})")
            return None

    # cast some numeric columns from str to numeric
    tables["stops"] = tables["stops"].astype({"stop_lon": float, "stop_lat": float})
    tables["stop_times"] = tables["stop_times"].astype({"stop_sequence": int})
    if tables.get("shapes") is not None:
        tables["shapes"] = tables["shapes"].astype(
            {"shape_pt_lon": float, "shape_pt_lat": float, "shape_pt_sequence": int}
        )

    # Set null values on optional columns used in this module.
    null_columns = {
        ("stops", "parent_station"): "nan",
        ("agency", "agency_id"): None,
    }
    for key, value in null_columns.items():
        table, col = key
        if table in tables and col not in tables[table].columns:
            tables[table][col] = value

    return tables

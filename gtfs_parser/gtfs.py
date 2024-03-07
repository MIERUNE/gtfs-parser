import glob
import os
import zipfile
import pandas as pd
import io


def append_table(f: io.BufferedIOBase, table_path: str, table_dfs: dict):
    datatype = os.path.splitext(os.path.basename(table_path))[0]
    df = pd.read_csv(f, dtype=str)
    if len(df) == 0:
        print(f"{datatype}.txt is empty, skipping...")
        pass
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
        try:
            with zipfile.ZipFile(path) as z:
                for file_name in z.namelist():
                    if file_name.endswith(".txt"):
                        with z.open(file_name) as f:
                            append_table(f, file_name, tables)
        except Exception as e:
            print(f"zip file read error({path}: {str(e)}")
            raise e

    # cast some numeric columns from str to numeric
    tables["stops"] = tables["stops"].astype({"stop_lon": float, "stop_lat": float})
    tables["stop_times"] = tables["stop_times"].astype({"stop_sequence": int})
    if tables.get("shapes") is not None:
        tables["shapes"] = tables["shapes"].astype(
            {"shape_pt_lon": float, "shape_pt_lat": float, "shape_pt_sequence": int}
        )

    # parent_station is optional column on GTFS but use in this module
    # when parent_station is not in stops, fill by 'nan' (not NaN)
    if "parent_station" not in tables.get("stops").columns:
        tables["stops"]["parent_station"] = "nan"

    return tables

"""
Combine all CSV files in the 'universe 2025' folder into a single Excel file: universe2025.xlsx
Reads one CSV at a time and appends rows directly to Excel. Never loads the full dataset into memory.
Single heading, dedupe by Date+Ticker. Output order is file order (no global sort).
"""

import os
import pandas as pd
import glob
from openpyxl import Workbook

# Folder containing universe 2025 CSV files (relative to script directory)
UNIVERSE_2025_FOLDER = "universe 2025"
OUTPUT_FILE = "universe2025.xlsx"


def _drop_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows that exactly match the column names (repeated header)."""
    row_matches_header = (
        df.astype(str).fillna("").eq(df.columns.astype(str), axis=1).all(axis=1)
    )
    return df[~row_matches_header]


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(script_dir, UNIVERSE_2025_FOLDER)

    if not os.path.isdir(folder_path):
        print(f"Folder not found: {folder_path}")
        print(f"Create a folder named '{UNIVERSE_2025_FOLDER}' and add your CSV files.")
        return

    # Find all CSV files (including in subfolders)
    csv_pattern = os.path.join(folder_path, "**", "*.csv")
    data_files = glob.glob(csv_pattern, recursive=True)
    data_files = [f for f in data_files if not os.path.basename(f).startswith("~")]
    data_files.sort()

    if not data_files:
        print(f"No .csv files found in {folder_path}")
        return

    print(f"Found {len(data_files)} file(s) in '{UNIVERSE_2025_FOLDER}'")
    output_path = os.path.join(script_dir, OUTPUT_FILE)

    key_cols = None
    seen_keys = set()
    columns_order = None
    total_rows_written = 0

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Sheet", 0)

    for path in data_files:
        try:
            try:
                df = pd.read_csv(path, header=0, encoding="utf-8-sig")
            except UnicodeDecodeError:
                df = pd.read_csv(path, header=0, encoding="latin-1")

            df = _drop_header_rows(df)
            if df.empty:
                print(f"  Read {os.path.basename(path)}: 0 rows (after dropping header rows)")
                continue

            if key_cols is None:
                key_cols = [c for c in ["Date", "Ticker"] if c in df.columns]

            if key_cols:
                keys = df[key_cols].fillna("").astype(str).apply(tuple, axis=1)
                new_mask = ~keys.isin(seen_keys)
                df = df.loc[new_mask].copy()
                seen_keys.update(keys[new_mask].tolist())

            if df.empty:
                print(f"  Read {os.path.basename(path)}: 0 new rows (after dedupe)")
                continue

            if columns_order is None:
                columns_order = list(df.columns)
                ws.append(columns_order)
            else:
                df = df.reindex(columns=columns_order, fill_value="")

            for row in df.itertuples(index=False):
                ws.append(list(row))

            total_rows_written += len(df)
            print(f"  Read {os.path.basename(path)}: {len(df)} rows written to Excel")

        except Exception as e:
            print(f"  Skip {os.path.basename(path)}: {e}")

    wb.save(output_path)

    if total_rows_written == 0:
        print("No data loaded.")
        try:
            os.remove(output_path)
        except OSError:
            pass
        return

    print(f"\nSaved {total_rows_written} rows to {OUTPUT_FILE}")
    print(f"Full path: {output_path}")


if __name__ == "__main__":
    main()

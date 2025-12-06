import pandas as pd
from pathlib import Path

# 🔧 Folder where your CSVs live
CSV_DIR = Path("data")

print(f"Scanning CSV files in: {CSV_DIR.resolve()}\n")

for csv_path in sorted(CSV_DIR.glob("*.csv")):
    try:
        print("=" * 80)
        print(f"📁 File: {csv_path.name}")

        # Read CSV
        df = pd.read_csv(csv_path)

        # Basic info
        print(f"   ➤ Rows: {len(df)}")
        print(f"   ➤ Columns: {len(df.columns)}")
        print(f"   ➤ Column names: {list(df.columns)}")

        # Optional: uncomment if you also want null counts
        # print("\n   🔎 Null / blank values per column:")
        # print(df.isnull().sum())

    except Exception as e:
        print(f"   ❌ Error reading {csv_path.name}: {e}")

print("\n✅ Finished scanning all CSV files.")


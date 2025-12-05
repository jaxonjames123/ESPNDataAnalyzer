from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent


DB_FOLDER = PROJECT_ROOT / "espn_analytics" / "duckdb"
DB_FOLDER.mkdir(parents=True, exist_ok=True)
DB_FILE = DB_FOLDER / "espn.duckdb"
RAW_CSV = PROJECT_ROOT / "raw" / "athletes" / "all_player_basic_data.csv"

con = duckdb.connect(database=str(DB_FILE), read_only=False)

con.execute(f"""
    CREATE OR REPLACE VIEW main.players AS
    SELECT * FROM read_csv_auto('{RAW_CSV}')
""")

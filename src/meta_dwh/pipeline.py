"""Pipeline runner for the Meta DWH, runs the SQL files of each layer in file name order"""
from pathlib import Path
import duckdb
from datetime import date
from meta_dwh.generate_events import generate_events, write_raw_events


PROJECT_ROOT = Path(__file__).resolve().parents[2] # The root of the project
SQL_DIR = PROJECT_ROOT / "sql"
DB_PATH = PROJECT_ROOT / "data" / "meta_dwh.duckdb"

# Demo parameters
DEMO_SEED = 42
DEMO_START_DATE = date(2026,10,20)
DEMO_DAYS = 21
DEMO_EVENTS_PER_DAY = 200


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Open the Meta DWH database with the session time zone set to UTC."""
    db_path.parent.mkdir(parents=True, exist_ok=True)  # create data/ if missing
    con = duckdb.connect(str(db_path))
    con.execute("SET TimeZone = 'UTC'")
    return con

def run_sql_folder(con: duckdb.DuckDBPyConnection, folder: Path) -> None:
    """Run every .sql file in folder, in file-name order."""
    for sql_file in sorted(folder.glob("*.sql")):
        print(f"Running {sql_file.name}")
        con.execute(sql_file.read_text())


def main() -> None:
    """Rebuild the local Meta DWH from scratch: tables, simulated ingestion, transforms."""
    DB_PATH.unlink(missing_ok=True)  # delete the old database file, if any
    con = connect(DB_PATH)
    run_sql_folder(con, SQL_DIR / "ddl")
    rows = generate_events(DEMO_SEED, DEMO_START_DATE, DEMO_DAYS, DEMO_EVENTS_PER_DAY)
    write_raw_events(con, rows)
    run_sql_folder(con, SQL_DIR / "transform")
    print(con.sql("SHOW TABLES"))
    con.close()


if __name__ == "__main__":
    main()
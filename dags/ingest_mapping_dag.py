from airflow.decorators import dag, task
from datetime import datetime
from assets import raw_mapping_asset

@dag(
        schedule = '@daily',
        start_date = datetime(2026, 1, 1),
        catchup = False,
)
def ingest_mapping():
    
    @task
    def fetch_mapping():
        import requests
        import json
        from pathlib import Path
        from datetime import datetime, timezone

        url = "https://prices.runescape.wiki/api/v1/osrs/mapping"
        headers = {"User-Agent": "osrs-ge-tracker (personal portfolio project)"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        output_dir = Path("/opt/airflow/data/raw/mapping")
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        output_path = output_dir / f"{timestamp}.json"
        output_path.write_text(response.text)

        return str(output_path)

    @task(pool = 'duckdb_writer', outlets = [raw_mapping_asset])
    def load_mapping_to_duckdb(file_path: str):
        import duckdb
        try:
            con = duckdb.connect("/opt/airflow/data/osrs.duckdb")
            con.execute(
                """
                CREATE OR REPLACE TABLE raw_mapping AS
                SELECT *, now() AS loaded_at
                FROM read_json_auto(?)
                """,
                [file_path],
            )
        finally:
            con.close()

    file_path = fetch_mapping()
    load_mapping_to_duckdb(file_path)


ingest_mapping()
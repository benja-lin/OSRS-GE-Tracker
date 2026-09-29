from airflow.decorators import dag, task
from datetime import datetime
from assets import raw_5m_prices_asset


@dag(
    schedule='*/5 * * * *', 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs = 1
)
def ingest_5m_prices():

    @task
    def fetch_5m_prices():
        import requests
        from pathlib import Path
        from datetime import datetime, timezone

        url = "https://prices.runescape.wiki/api/v1/osrs/5m"
        headers = {"User-Agent": "osrs-ge-tracker (personal portfolio project)"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        output_dir = Path("/opt/airflow/data/raw/prices")
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        output_path = output_dir / f"{timestamp}.json"
        output_path.write_text(response.text)

        return str(output_path)

    @task(pool = 'duckdb_writer', outlets = [raw_5m_prices_asset])
    def load_5m_prices_to_duckdb(file_path: str):
        import duckdb

        con = duckdb.connect("/opt/airflow/data/osrs.duckdb")
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_5m_prices (
                    data JSON,
                    timestamp BIGINT,
                    loaded_at TIMESTAMP
                )
                """
            )

            con.execute(
                """
                INSERT INTO raw_5m_prices
                SELECT data, timestamp, now() AS loaded_at
                FROM read_json_auto(?)
                """,
                [file_path],
            )
        finally:
            con.close()

    file_path = fetch_5m_prices()
    load_5m_prices_to_duckdb(file_path)


ingest_5m_prices()
from airflow.decorators import dag, task
from airflow.sdk import Asset
from datetime import datetime

from assets import raw_mapping_asset, raw_5m_prices_asset


@dag(
    schedule=(raw_mapping_asset & raw_5m_prices_asset),
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def dbt_transform():

    @task
    def run_dbt():
        import subprocess

        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", "."],
            cwd="/opt/airflow/include",
            capture_output=True,
            text=True,
        )

        print(result.stdout)
        print(result.stderr)

        if result.returncode != 0:
            raise Exception(f"dbt run failed with return code {result.returncode}")

    run_dbt()


dbt_transform()
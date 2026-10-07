from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator 

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_PYTHON = PROJECT_ROOT / "venv" / "bin" / "python"

# Destination: orch/dag/market_pipeline.py
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_PYTHON = PROJECT_ROOT / "venv" / "bin" / "python"

with DAG(
    dag_id="indian_market_batch",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
) as dag:
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            'export JAVA_HOME=$(/usr/libexec/java_home -v 17) && '
            f'"{PROJECT_PYTHON}" processing/bronze_to_silver.py'
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            'export JAVA_HOME=$(/usr/libexec/java_home -v 17) && '
            f'"{PROJECT_PYTHON}" processing/silver_to_gold.py'
        ),
    )

    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            'export JAVA_HOME=$(/usr/libexec/java_home -v 17) && '
            f'"{PROJECT_PYTHON}" clickhouse/load_gold_to_ch.py'
        ),
    )

    bronze_to_silver >> silver_to_gold >> load_clickhouse
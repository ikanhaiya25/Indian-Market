#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PYTHON=${PYTHON:-"$PROJECT_ROOT/venv/bin/python"}

if [ ! -x "$PYTHON" ]; then
    echo "Python interpreter not found at $PYTHON. Set PYTHON or create the project venv." >&2
    exit 1
fi

if [ -x /usr/libexec/java_home ]; then
    JAVA_HOME=$(/usr/libexec/java_home -v 17)
elif [ -z "${JAVA_HOME:-}" ]; then
    echo "Java 17 is required for the project's PySpark 3.5 setup." >&2
    exit 1
fi

export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
cd "$PROJECT_ROOT"

docker compose up -d
curl --retry 30 --retry-connrefused --retry-delay 1 --retry-max-time 30 -fsS \
    http://localhost:8123/ping >/dev/null
docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/schema.sql

"$PYTHON" processing/bronze_to_silver.py
"$PYTHON" processing/silver_to_gold.py
"$PYTHON" clickhouse/load_gold_to_ch.py
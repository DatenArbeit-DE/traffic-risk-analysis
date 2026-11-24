#!/usr/bin/env bash
# Supervises the Airflow 3 components inside a single container.
#
# Airflow 3 replaced the monolithic `webserver` with `api-server` and moved DAG
# parsing into a separate `dag-processor`. Running all three here keeps the
# "webserver + scheduler in one container" requirement while staying on the
# current Airflow release. No triggerer: this DAG has no deferrable operators.
set -euo pipefail

echo "[entrypoint] applying Airflow metadata migrations"
airflow db migrate

echo "[entrypoint] ensuring admin user '${AIRFLOW_ADMIN_USER}'"
airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}" 2>/dev/null || echo "[entrypoint] admin user already exists"

shutdown() {
    echo "[entrypoint] stopping Airflow components"
    kill -TERM 0 2>/dev/null || true
}
trap shutdown TERM INT

echo "[entrypoint] starting api-server, scheduler, dag-processor"
airflow api-server --port 8080 &
airflow scheduler &
airflow dag-processor &

# Exit (and let Docker restart the container) as soon as any component dies.
wait -n
echo "[entrypoint] a component exited; shutting down so Docker can restart"

"""
Pipeline A — Airflow DAG

Orchestrates the medallion data pipeline, matching the DeltaTrace paper
architecture (Figure 1):

    check_bronze -> preprocess_data -> check_data -> if_new_data
                 -> request_ai_training -> compute_statistics
                 -> check_gold_quality -> data_exposition -> notify_done

Bronze runs as a persistent Spark Structured Streaming job and is NOT
triggered by this DAG. The DAG only verifies that Bronze has data, then
runs Silver (preprocess) and Gold (compute statistics) as batch jobs.

Phase 3 (AI training) and Phase 4 (data export) are placeholders --
currently echo-only. They will be wired up when FastAPI + MLflow and
Grafana + PostgreSQL sink are added.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# -- Container lookup (resolved at runtime by bash) --------------------------
# Uses `docker ps` filter so the DAG works regardless of compose project name.
SPARK = '$(docker ps -qf "name=spark-master" | head -1)'
HDFS = '$(docker ps -qf "name=namenode" | head -1)'


# -- Spark-submit templates --------------------------------------------------
# Shared flags match the Makefile SPARK_SUBMIT macro.
_SPARK_BASE = (
    f"docker exec {SPARK} sh -lc '"
    "mkdir -p /tmp/.ivy2 && "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--driver-memory 512m "
    "--conf spark.jars.ivy=/tmp/.ivy2 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "--conf spark.sql.ansi.enabled=false "
)

SILVER_CMD = (
    _SPARK_BASE
    + "--executor-memory 512m "
    + "--conf spark.executor.memoryOverhead=512m "
    + "--packages io.delta:delta-spark_2.12:3.2.0 "
    + "/opt/spark/work-dir/spark_silver.py'"
)

GOLD_CMD = (
    _SPARK_BASE
    + "--executor-memory 1g "
    + "--conf spark.executor.memoryOverhead=768m "
    + "--packages io.delta:delta-spark_2.12:3.2.0 "
    + "/opt/spark/work-dir/spark_gold.py'"
)


# -- HDFS existence checks ---------------------------------------------------
def _hdfs_has_data(path: str) -> str:
    return (
        f'test "$(docker exec {HDFS} hdfs dfs -ls {path} 2>/dev/null '
        "| grep -c '^d')\" -ge 1"
    )


CHECK_BRONZE_CMD = _hdfs_has_data("/data/bronze/wearable")
CHECK_SILVER_CMD = _hdfs_has_data("/data/silver/wearable")
CHECK_GOLD_CMD = _hdfs_has_data("/data/gold/wearable")


# -- DAG ---------------------------------------------------------------------
default_args = {
    "owner": "minh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_a",
    description="Medallion pipeline: Bronze check -> Silver -> Gold -> (AI) -> (Export)",
    default_args=default_args,
    schedule="0 1 * * *",          # 01:00 every day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pipeline_a", "medallion", "wearable"],
) as dag:

    # 1 - Verify Bronze has data (streaming job must be running)
    check_bronze = BashOperator(
        task_id="check_bronze_data",
        bash_command=CHECK_BRONZE_CMD,
    )

    # 2 - Preprocess: Bronze -> Silver
    preprocess_data = BashOperator(
        task_id="preprocess_data",
        bash_command=SILVER_CMD,
        execution_timeout=timedelta(minutes=30),
    )

    # 3 - Validate Silver output
    check_data = BashOperator(
        task_id="check_data",
        bash_command=CHECK_SILVER_CMD,
    )

    # 4 - Branch: has new data since last AI training?
    #     (Phase 3 placeholder -- currently always proceeds)
    if_new_data = EmptyOperator(
        task_id="if_new_data",
    )

    # 5 - Request AI training: call FastAPI /train endpoint.
    #     FastAPI runs training in a background thread and returns {job_id, status}
    #     immediately, so this task doesn't block on the full training run.
    #     Airflow resolves the `fastapi` service name via the compose network.
    request_ai_training = BashOperator(
        task_id="request_ai_training",
        bash_command='curl -sf -X POST http://fastapi:8000/train',
    )

    # 6 - Compute statistics: Silver -> Gold
    compute_statistics = BashOperator(
        task_id="compute_statistics",
        bash_command=GOLD_CMD,
        execution_timeout=timedelta(minutes=30),
    )

    # 7 - Validate Gold output
    check_gold_quality = BashOperator(
        task_id="check_gold_quality",
        bash_command=CHECK_GOLD_CMD,
    )

    # 8 - Export Gold -> dashboarding DB -- Phase 4 placeholder
    #     Will become: Spark job writing Gold Delta -> PostgreSQL sink for Grafana
    data_exposition = BashOperator(
        task_id="data_exposition",
        bash_command='echo "[DAG] Data export -- placeholder (Phase 4)"',
    )

    # 9 - Done
    notify_done = BashOperator(
        task_id="notify_done",
        bash_command='echo "[DAG] Pipeline A completed at $(date)"',
    )

    # Dependencies -- matches paper Figure 1
    (
        check_bronze
        >> preprocess_data
        >> check_data
        >> if_new_data
        >> request_ai_training
        >> compute_statistics
        >> check_gold_quality
        >> data_exposition
        >> notify_done
    )

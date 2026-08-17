"""
Airflow DAG: orchestrate a Spark job that writes to an Iceberg table.

The DockerOperator spawns a short-lived Spark container (same image as the
interactive one), bind-mounts ./spark/scripts, and runs spark-submit against it.
This demonstrates the full chain:  Airflow -> Spark -> Iceberg -> MinIO.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ${PWD} from docker-compose = this project's dir on the HOST. The DockerOperator
# talks to the host Docker daemon, so bind-mount sources must be host paths.
HOST_PROJECT_PATH = os.environ["HOST_PROJECT_PATH"]

with DAG(
    dag_id="spark_iceberg_etl",
    description="Airflow triggers Spark to write an Iceberg table in MinIO",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # trigger manually from the UI
    catchup=False,
    tags=["demo", "spark", "iceberg"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_spark_etl = DockerOperator(
        task_id="run_spark_etl",
        image="tabulario/spark-iceberg:latest",
        # Reach iceberg-rest & minio by service name on the shared network.
        network_mode="iceberg_lab_net",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        auto_remove="success",
        # IMPORTANT with a "remote" daemon: don't try to mount a local tmp dir.
        mount_tmp_dir=False,
        environment={
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
        },
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_PATH}/spark/scripts",
                target="/opt/scripts",
                type="bind",
            )
        ],
        command="spark-submit --master local[*] /opt/scripts/etl.py",
    )

    start >> run_spark_etl

# iceberg-lab

A self-contained local lab that runs the full modern-data-stack chain on your laptop:

```
Airflow  ──DockerOperator──>  Spark  ──>  Iceberg tables  ──>  Parquet in MinIO (S3)
                                            │
                                    Iceberg REST catalog
                                   (metadata source of truth)
```

Airflow schedules the work but does no data processing itself. Its DAG launches a
short-lived Spark container on the host Docker daemon, which runs `spark-submit`
against an Iceberg table whose data files live in MinIO and whose metadata is
tracked by an Iceberg REST catalog.

## Services

| Service | URL | Credentials | Purpose |
|---|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` | Orchestration UI |
| Jupyter (Spark) | http://localhost:8888 | — | Interactive Spark + Iceberg |
| Spark UI | http://localhost:4040 | — | Only live while a job runs |
| MinIO console | http://localhost:9001 | `admin` / `password` | Browse the `warehouse` bucket |
| MinIO S3 API | http://localhost:9000 | `admin` / `password` | What Spark writes to |
| Iceberg REST catalog | http://localhost:8181 | — | Table metadata |

Postgres (Airflow's metadata DB) runs internally only and is not published to the host.

These are throwaway local credentials, hardcoded in `docker-compose.yml` and the DAG.
Don't carry this pattern into anything real.

## Make targets

A `Makefile` wraps the commands you'll repeat. `make` on its own lists them all:

```
make init        # one-time: create .env, migrate the DB, add the admin user
make up          # start the stack
make trigger     # unpause + trigger the ETL DAG, wait for it to finish
make task-log    # the Spark job's output from the most recent run
make count       # row count of demo.sales.orders
make snapshots   # Iceberg snapshot history
make warehouse   # list the Parquet + metadata files in MinIO
make sql Q="SELECT ..."   # arbitrary Spark SQL (catalog config included)
make clean       # stop the stack and delete all data
make clean-logs  # delete the local Airflow logs in logs/
```

`clean` drops the Docker volumes; `logs/` is a bind mount and survives it, so
`make clean clean-logs` is the full reset.

The `sql`/`count`/`snapshots` targets pass the catalog `--conf` overrides for you,
which is why they work where a bare `spark-sql` doesn't — see the caveat below.

The rest of this README spells out the underlying commands.

## Prerequisites

Docker Desktop (or Docker Engine + Compose v2). The Airflow scheduler mounts
`/var/run/docker.sock` so it can start Spark containers on your host daemon.

## Setup

```bash
cd iceberg-lab
cp env.example .env
# Linux only — on macOS/Docker Desktop leave AIRFLOW_UID at 50000:
#   echo "AIRFLOW_UID=$(id -u)" >> .env
#   echo "DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)" >> .env
docker compose up airflow-init             # one-time DB init + admin user
docker compose up -d
```

`airflow-init` is a one-shot container: it runs `airflow db migrate`, creates the
admin user, and exits 0. The webserver and scheduler wait for it to complete.

## Run the ETL

1. Open http://localhost:8080 and log in as `airflow` / `airflow`.
2. Unpause the **`spark_iceberg_etl`** DAG (DAGs start paused by design).
3. Trigger it. Watch the `run_spark_etl` task log — the spawned Spark container's
   stdout is streamed back into Airflow.

The job (`spark/scripts/etl.py`) creates namespace `demo.sales`, creates table
`demo.sales.orders` partitioned by `days(order_ts)`, appends three rows, reads
them back, and prints the table's snapshot history. Each write is a new Iceberg
snapshot you can time-travel to with `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`.

Re-triggering the DAG appends another batch and adds another snapshot — the
table is created with `IF NOT EXISTS`, so runs accumulate rather than reset.

### Inspect the results

Confirm the Parquet and metadata files landed in the MinIO console
(http://localhost:9001, bucket `warehouse`). You should see the partition layout
Iceberg created, plus its metadata tree:

```
sales/orders/data/order_ts_day=2024-01-01/00000-...parquet
sales/orders/data/order_ts_day=2024-01-02/00000-...parquet
sales/orders/metadata/00002-....metadata.json
sales/orders/metadata/snap-....avro
```

You can also run the Spark job by hand, without Airflow. `etl.py` configures its
own catalog explicitly, so this works as-is:

```bash
docker compose exec spark-iceberg spark-submit /opt/scripts/etl.py
```

To query the tables interactively you must override the catalog config — see the
caveat below:

```bash
docker compose exec spark-iceberg spark-sql \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.type=rest \
  --conf spark.sql.catalog.demo.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.demo.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.demo.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.demo.s3.path-style-access=true \
  -e "SELECT * FROM demo.sales.orders ORDER BY order_id"
```

> **The interactive container's built-in `demo` catalog does not work in this stack.**
> `tabulario/spark-iceberg` ships a `spark-defaults.conf` whose `demo` catalog points
> at `http://rest:8181`, but this compose file names the service `iceberg-rest`, so the
> hostname doesn't resolve. It also defaults the warehouse to `s3://warehouse/wh/`
> rather than `s3://warehouse/`, and omits `s3.path-style-access`, which MinIO needs.
> Pass the `--conf` flags above (they mirror what `etl.py` sets), or give `iceberg-rest`
> a `rest` network alias in `docker-compose.yml` to fix it at the source. The same
> caveat applies to notebooks in Jupyter on port 8888.

## Repository layout

```
docker-compose.yml        Every service; x-airflow-common holds the shared Airflow config
env.example               Copy to .env — AIRFLOW_UID and DOCKER_GID
dags/
  spark_iceberg_etl.py    The DAG — bind-mounts spark/scripts and runs spark-submit
spark/scripts/
  etl.py                  The Spark job — REST catalog + MinIO, writes demo.sales.orders
plugins/                  Bind-mounted into every Airflow container (empty)
logs/                     Airflow runtime output (gitignored)
```

File placement matters here. `dags/` is bind-mounted to `/opt/airflow/dags`, and the
DAG mounts `spark/scripts` into the Spark container as `/opt/scripts`. A script
elsewhere in the tree will not be picked up.

The DAG resolves its bind-mount source from `HOST_PROJECT_PATH` (set to `${PWD}` in
`docker-compose.yml`). Because the DockerOperator talks to the *host* daemon, mount
sources must be host paths, not container paths.

The compose network name is pinned to `iceberg_lab_net` so the spawned Spark
container can attach to it and reach `iceberg-rest` and `minio` by service name.

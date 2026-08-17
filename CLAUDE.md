# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A local Docker Compose lab demonstrating Airflow → Spark → Iceberg → MinIO. There is
no application code to build, no test suite, and no linter — "running the code" means
bringing up the stack and triggering the DAG. Verification is therefore always
empirical: trigger a run and inspect the resulting table and object store.

## Commands

The `Makefile` is the intended interface; `make` with no target lists everything.

```bash
make init          # one-time: create .env, migrate the Airflow DB, add the admin user
make up            # start the stack
make trigger       # unpause + trigger the DAG, block until it reaches a terminal state
make task-log      # Spark job output from the most recent run (INFO/WARN stripped)
make count         # row count of demo.sales.orders
make snapshots     # Iceberg snapshot history
make warehouse     # list Parquet + metadata files in MinIO
make sql Q="..."   # arbitrary Spark SQL
make clean         # down -v: deletes all Docker volumes
make clean-logs    # empties logs/ (a bind mount, so `clean` does not touch it)
```

`make init` must precede `make up` on a fresh checkout or after `make clean` — the
Airflow metadata DB lives in a volume and needs migrating before the webserver and
scheduler will start.

The full verification loop after changing the DAG or the Spark job:
`make trigger && make task-log && make count && make snapshots`.

If the DAG doesn't appear in the UI, check `docker compose logs airflow-scheduler`.
`make dag-errors` surfaces import failures more directly; empty output there means the
DAGs parsed, so the cause is elsewhere — usually file placement, or the scheduler not
having finished starting up.

## Architecture

The execution chain crosses three hosts, which explains most of the config:

1. The **Airflow scheduler container** runs the DAG, mounting `/var/run/docker.sock`.
2. Its `DockerOperator` talks to the **host** Docker daemon and starts an ephemeral
   Spark container there.
3. That container runs `spark-submit` against the Iceberg REST catalog and MinIO.

Consequences worth internalizing:

- **Bind-mount sources must be host paths.** `HOST_PROJECT_PATH` is set to `${PWD}` in
  `docker-compose.yml` and read by the DAG, because a path valid inside the scheduler
  container is meaningless to the host daemon.
- **The network name is pinned** to `iceberg_lab_net` so the spawned container can
  attach to it and resolve `iceberg-rest` and `minio` by service name.
- **`DOCKER_GID` must match the docker socket's group** for the scheduler to start
  containers. The default `999` is verified working on Docker Desktop; on Linux set it
  in `.env` to `stat -c '%g' /var/run/docker.sock` and recreate the containers. A
  mismatch shows up as a permission error on `/var/run/docker.sock` in the task log.
- **File placement is load-bearing.** `dags/` is bind-mounted to `/opt/airflow/dags`,
  and the DAG mounts `spark/scripts` into the Spark container as `/opt/scripts`. A
  script anywhere else is simply never loaded.
- The Airflow Docker provider is installed at container start via
  `_PIP_ADDITIONAL_REQUIREMENTS`, not baked into an image. The scheduler takes ~30s to
  become useful after `up`, and adding providers means editing that variable.

`spark/scripts/etl.py` is idempotent in structure but cumulative in data: the table is
created `IF NOT EXISTS` and each run `INSERT`s the same three rows, so every trigger
adds a snapshot and three more rows. A first run on a clean stack yields exactly 3 rows
and 1 snapshot — the fastest way to confirm a rebuild really started from nothing.

## Gotchas that have already cost time

These are all fixed in the repo; do not regress them.

- **`DockerOperator.command` must be a single-element list**, not a string. The
  `tabulario/spark-iceberg` entrypoint ends with `eval "$1"`, so it runs only its first
  argument. A string gets shlex-split and `spark-submit` receives no script, printing a
  usage banner and exiting 255.
- **The interactive container's built-in `demo` catalog is broken in this stack.** The
  image's `spark-defaults.conf` points it at `http://rest:8181` (this compose file names
  the service `iceberg-rest`) with warehouse `s3://warehouse/wh/` and no
  `s3.path-style-access`. A bare `spark-sql` fails with `UnknownHostException: rest`.
  The `ICEBERG_CONF` variable in the `Makefile` holds the overrides that fix it; use
  `make sql` rather than reconstructing them.
- **`AIRFLOW_UID` must stay `50000` on macOS/Windows.** The `airflow-init` service
  overrides the image entrypoint, which is what would normally register an arbitrary uid
  in `/etc/passwd`; a host uid there makes init die with `getpwuid(): uid not found`.
  Setting it to `$(id -u)` is a Linux-only fix for bind-mount file ownership. To make a
  non-default uid work, drop `entrypoint: /bin/bash` from `airflow-init` so the image's
  own entrypoint runs, and move the steps into the command instead:
  `command: ["bash", "-c", "airflow db migrate && airflow users create ..."]`.
- **Never delete the `logs/` directory itself**, only its contents. Docker recreates a
  missing bind-mount source as root-owned, reintroducing the ownership problem above.

## Conventions

Commit messages use Conventional Commits: `type(scope): subject`, with scopes drawn from
the area touched (`make`, `dag`, `compose`, `spark`). The first three commits in this
repo predate that decision; leave them alone rather than rewriting pushed history.

Credentials are hardcoded throughout (`airflow`/`airflow`, `admin`/`password`) and are
deliberate for a throwaway local lab.

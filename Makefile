# iceberg-lab — development shortcuts.
# Run `make` or `make help` for the full list.

COMPOSE  := docker compose
DAG_ID   := spark_iceberg_etl
TASK_ID  := run_spark_etl
NETWORK  := iceberg_lab_net
MC_IMAGE := minio/mc:RELEASE.2024-11-05T11-29-45Z
OPEN     := $(shell command -v open 2>/dev/null || command -v xdg-open 2>/dev/null || echo echo)

# The interactive spark-iceberg image ships a `demo` catalog pointing at
# http://rest:8181 with warehouse s3://warehouse/wh/, neither of which exists in
# this stack. These overrides mirror what spark/scripts/etl.py configures.
ICEBERG_CONF := \
	--conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
	--conf spark.sql.catalog.demo.type=rest \
	--conf spark.sql.catalog.demo.uri=http://iceberg-rest:8181 \
	--conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
	--conf spark.sql.catalog.demo.warehouse=s3://warehouse/ \
	--conf spark.sql.catalog.demo.s3.endpoint=http://minio:9000 \
	--conf spark.sql.catalog.demo.s3.path-style-access=true

# Overridable on the command line, e.g. `make sql Q="SELECT count(*) FROM ..."`
Q      ?= SELECT * FROM demo.sales.orders ORDER BY order_id
RUN_ID ?= make_$(shell date +%Y%m%d_%H%M%S)

.DEFAULT_GOAL := help
.PHONY: help init up down restart clean clean-logs ps config \
        logs logs-scheduler logs-webserver \
        dags dag-errors trigger wait task-log runs \
        etl sql count snapshots warehouse verify \
        shell-spark shell-airflow ui minio-ui

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

help: ## Show this help
	@echo "iceberg-lab — usage: make <target>"
	@echo
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Variables:  Q=\"SELECT ...\"  RUN_ID=<id>  SERVICE=<compose service>"

# ---------------------------------------------------------------------------
# Stack lifecycle
# ---------------------------------------------------------------------------

.env:
	@cp env.example .env
	@echo "Created .env from env.example"

init: .env ## One-time setup: create .env, migrate the Airflow DB, add the admin user
	$(COMPOSE) up airflow-init

up: .env ## Start the whole stack in the background
	$(COMPOSE) up -d

down: ## Stop the stack, keep data
	$(COMPOSE) down

restart: ## Restart the stack (SERVICE=airflow-scheduler to narrow)
	$(COMPOSE) restart $(SERVICE)

clean: ## Stop the stack and DELETE all data (MinIO, Postgres, notebooks)
	$(COMPOSE) down -v

clean-logs: ## Delete Airflow's task/scheduler logs from logs/
	@mkdir -p logs
	@find logs -mindepth 1 -delete
	@echo "Cleared logs/"

ps: ## Show container status
	@$(COMPOSE) ps -a --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}'

config: ## Validate docker-compose.yml
	@$(COMPOSE) config -q && echo "docker-compose.yml OK"

# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

logs: ## Follow container logs (SERVICE=airflow-scheduler to narrow)
	$(COMPOSE) logs -f $(SERVICE)

logs-scheduler: ## Follow the Airflow scheduler log
	$(COMPOSE) logs -f airflow-scheduler

logs-webserver: ## Follow the Airflow webserver log
	$(COMPOSE) logs -f airflow-webserver

# ---------------------------------------------------------------------------
# Airflow / the DAG
# ---------------------------------------------------------------------------

dags: ## List parsed DAGs
	@$(COMPOSE) exec -T airflow-scheduler airflow dags list

dag-errors: ## Show DAG import errors (empty output means the DAGs parse)
	@$(COMPOSE) exec -T airflow-scheduler airflow dags list-import-errors

runs: ## List recent runs of the ETL DAG
	@$(COMPOSE) exec -T airflow-scheduler airflow dags list-runs -d $(DAG_ID) -o plain

trigger: ## Unpause and trigger the ETL DAG, then wait for it to finish
	@$(COMPOSE) exec -T airflow-scheduler airflow dags unpause $(DAG_ID) >/dev/null
	@$(COMPOSE) exec -T airflow-scheduler airflow dags trigger $(DAG_ID) -r $(RUN_ID) >/dev/null
	@echo "Triggered run $(RUN_ID)"
	@$(MAKE) --no-print-directory wait RUN_ID=$(RUN_ID)

wait: ## Wait for RUN_ID to reach a terminal state
	@for i in $$(seq 1 60); do \
		state=$$($(COMPOSE) exec -T airflow-scheduler airflow dags list-runs -d $(DAG_ID) -o plain 2>/dev/null \
			| awk -v r="$(RUN_ID)" '$$2 == r { print $$3 }'); \
		case "$$state" in \
			success) echo; echo "run $(RUN_ID): success"; exit 0 ;; \
			failed)  echo; echo "run $(RUN_ID): FAILED — see 'make task-log'"; exit 1 ;; \
			*)       printf "\r  %s... (%ss)" "$${state:-queued}" $$((i * 5)) ;; \
		esac; \
		sleep 5; \
	done; \
	echo; echo "timed out waiting for $(RUN_ID)"; exit 1

task-log: ## Show the Spark container output from the most recent task run
	@f=$$(ls -t logs/dag_id=$(DAG_ID)/run_id=*/task_id=$(TASK_ID)/attempt=*.log 2>/dev/null | head -1); \
	if [ -z "$$f" ]; then echo "no task logs yet — run 'make trigger'"; exit 1; fi; \
	echo "== $$f"; \
	sed -E 's/^.*\{docker\.py:[0-9]+\} INFO - //' "$$f" \
		| grep -vE '^\[20[0-9]{2}-|[0-9]{2}/[0-9]{2}/[0-9]{2} [0-9:]+ (INFO|WARN|DEBUG) ' \
		| tail -40

# ---------------------------------------------------------------------------
# Spark / Iceberg
# ---------------------------------------------------------------------------

etl: ## Run the Spark job directly, bypassing Airflow
	$(COMPOSE) exec -T spark-iceberg spark-submit /opt/scripts/etl.py

sql: ## Run a Spark SQL query (Q="SELECT ...")
	@$(COMPOSE) exec -T spark-iceberg spark-sql $(ICEBERG_CONF) -e "$(Q)"

count: ## Row count of demo.sales.orders
	@$(MAKE) --no-print-directory sql Q="SELECT count(*) AS rows FROM demo.sales.orders"

snapshots: ## Iceberg snapshot history for demo.sales.orders
	@$(MAKE) --no-print-directory sql \
		Q="SELECT snapshot_id, committed_at, operation FROM demo.sales.orders.snapshots"

warehouse: ## List the Iceberg files MinIO is holding
	@docker run --rm --network $(NETWORK) --entrypoint sh $(MC_IMAGE) -c \
		"mc alias set local http://minio:9000 admin password >/dev/null && mc ls -r local/warehouse"

verify: ## End-to-end check: trigger the DAG, then confirm rows and snapshots
	@$(MAKE) --no-print-directory trigger
	@$(MAKE) --no-print-directory count
	@$(MAKE) --no-print-directory snapshots

# ---------------------------------------------------------------------------
# Shells and UIs
# ---------------------------------------------------------------------------

shell-spark: ## Open a shell in the Spark container
	$(COMPOSE) exec spark-iceberg bash

shell-airflow: ## Open a shell in the Airflow scheduler
	$(COMPOSE) exec airflow-scheduler bash

ui: ## Open the Airflow UI (airflow / airflow)
	@$(OPEN) http://localhost:8080

minio-ui: ## Open the MinIO console (admin / password)
	@$(OPEN) http://localhost:9001

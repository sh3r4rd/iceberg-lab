## Run the project

```bash
cd iceberg-lab
cp env.example .env
# Linux only — on macOS/Docker Desktop leave AIRFLOW_UID at 50000:
#   echo "AIRFLOW_UID=$(id -u)" >> .env
#   echo "DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)" >> .env
docker compose up airflow-init             # one-time DB init + admin user
docker compose up -d
```
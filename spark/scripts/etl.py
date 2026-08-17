"""
Spark job: create an Iceberg table, write rows, then demonstrate Iceberg
features (snapshots / time travel). Configured to use the REST catalog + MinIO.

Run by Airflow via DockerOperator, or manually:
    docker compose exec spark-iceberg spark-submit /opt/scripts/etl.py
"""

from pyspark.sql import SparkSession

CATALOG = "demo"

spark = (
    SparkSession.builder.appName("airflow-iceberg-etl")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
    .config(f"spark.sql.catalog.{CATALOG}.uri", "http://iceberg-rest:8181")
    .config(
        f"spark.sql.catalog.{CATALOG}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://warehouse/")
    .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
    .config("spark.sql.defaultCatalog", CATALOG)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Namespace + table (schema evolution & partitioning are Iceberg's job).
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.sales")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.sales.orders (
        order_id   BIGINT,
        customer   STRING,
        amount     DOUBLE,
        order_ts   TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(order_ts))
    """
)

# 2. Append a batch of rows (a new snapshot).
spark.sql(
    f"""
    INSERT INTO {CATALOG}.sales.orders VALUES
        (1, 'acme',    120.50, TIMESTAMP '2024-01-01 09:15:00'),
        (2, 'globex',   75.00, TIMESTAMP '2024-01-01 14:20:00'),
        (3, 'initech', 340.99, TIMESTAMP '2024-01-02 11:05:00')
    """
)

# 3. Read it back.
print("=== Current rows ===")
spark.table(f"{CATALOG}.sales.orders").orderBy("order_id").show(truncate=False)

count = spark.table(f"{CATALOG}.sales.orders").count()
print(f"=== Row count: {count} ===")

# 4. Iceberg superpower: inspect snapshots (each write is a versioned snapshot
#    you can time-travel to with FOR VERSION AS OF / FOR TIMESTAMP AS OF).
print("=== Snapshots (metadata table) ===")
spark.sql(
    f"SELECT snapshot_id, committed_at, operation "
    f"FROM {CATALOG}.sales.orders.snapshots"
).show(truncate=False)

spark.stop()
print("ETL complete.")

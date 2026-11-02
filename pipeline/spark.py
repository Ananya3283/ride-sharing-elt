"""SparkSession wired for the S3-compatible lake (s3a) and Postgres (JDBC)."""

import os

from pyspark.sql import SparkSession

from pipeline.config import Settings


def get_spark(settings: Settings, app_name: str = "ride-sharing-elt") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        # TLC timestamps are NYC wall-clock times with no zone. Treat them as UTC everywhere
        # (Spark SQL and the JVM's JDBC writer) so no step shifts them by the machine's timezone.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(settings.s3_endpoint.startswith("https")).lower(),
        )
    )
    if jars := os.environ.get("SPARK_JARS"):
        builder = builder.config("spark.jars", jars)
    return builder.getOrCreate()

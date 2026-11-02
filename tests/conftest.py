import os
import shutil

import pytest


@pytest.fixture(scope="session")
def spark():
    if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
        pytest.skip("Java not installed; Spark tests run in CI and in the Airflow container")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

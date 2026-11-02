"""DAG integrity checks. Needs Airflow, so run inside the container:
docker compose exec airflow-scheduler pytest /opt/airflow/tests/test_dag.py
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_task_order(dagbag):
    dag = dagbag.get_dag("ride_sharing_elt")
    assert dag is not None
    assert dag.get_task("extract").downstream_task_ids == {"spark_transform"}
    assert dag.get_task("spark_transform").downstream_task_ids == {"load"}
    assert dag.get_task("load").downstream_task_ids == {"dbt_run"}
    assert dag.get_task("dbt_run").downstream_task_ids == {"dbt_test"}

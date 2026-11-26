"""
DAG integrity tests. Require Airflow, so they run inside the Astro container
(`astro dev pytest`) or the CI `dags` job, and are skipped elsewhere.
"""

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

EXPECTED_DAGS = {
    "01_fakeshop_extract",
    "02_fakeshop_transform",
    "03_fakeshop_load_duckdb",
}


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_expected_dags_are_registered(dag_bag):
    assert set(dag_bag.dag_ids) == EXPECTED_DAGS


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dag_conventions(dag_bag, dag_id):
    dag = dag_bag.dags[dag_id]
    assert dag.tags, "every DAG needs tags"
    assert dag.default_args.get("retries", 0) >= 2
    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_load_dag_gates_on_data_quality(dag_bag):
    dag = dag_bag.dags["03_fakeshop_load_duckdb"]
    load = dag.get_task("load_all_dates")
    assert "validate_data_quality" in load.upstream_task_ids
    # Skipped (quarantined) partitions must not block the healthy ones.
    assert load.trigger_rule == "none_failed"
    # Marts are rebuilt (and dbt tests run) only after the load.
    assert dag.get_task("dbt_build").upstream_task_ids == {"load_all_dates"}


def _trigger_uris(dag):
    return {uri for uri, _ in dag.dataset_triggers.iter_datasets()}


def test_datasets_chain_the_dags(dag_bag):
    extract = dag_bag.dags["01_fakeshop_extract"]
    transform = dag_bag.dags["02_fakeshop_transform"]
    load = dag_bag.dags["03_fakeshop_load_duckdb"]

    raw_out = {d.uri for t in extract.tasks for d in t.outlets}
    processed_out = {d.uri for t in transform.tasks for d in t.outlets}

    assert raw_out & _trigger_uris(transform)
    assert processed_out & _trigger_uris(load)

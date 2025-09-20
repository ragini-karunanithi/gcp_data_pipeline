import pytest
from airflow.models import DagBag

def test_dag_loaded():
    dagbag = DagBag(dag_folder="./airflow/dags", include_examples=False)
    assert "medallion_composer_pipeline_csv_inline" in dagbag.dags

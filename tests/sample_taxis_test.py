from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from i_need_a_gay_group import taxis


def test_find_all_taxis():
    results = taxis.find_all_taxis()
    assert results.count() > 5

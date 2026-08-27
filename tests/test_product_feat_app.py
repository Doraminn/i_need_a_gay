import product_feat_agg
from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()

def test_check_count_rows_product(spark):
    sales_transaction = spark.table("samples.bakehouse.sales_transactions")
    franchise_master = spark.table("samples.bakehouse.sales_franchises")
    assert product_feat_agg.create_result_top_product(sales_transaction, franchise_master).count()==48

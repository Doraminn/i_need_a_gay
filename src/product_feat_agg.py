from pyspark.sql import functions as F, Window

def load_sales_transaction():
    return spark.table("samples.bakehouse.sales_transactions"
)
    
def load_sales_franchise():
    return spark.table("samples.bakehouse.sales_franchises")

def create_result_top_product(sales_transaction, franchise_master):
    window_spec = Window.partitionBy("franchiseID").orderBy(F.desc("total_revenue_from_product"))
    top_product_sales = (
        sales_transaction
        .groupBy("franchiseID", "product")
        .agg(
            F.sum("quantity").alias("quantity_sales"),
            F.sum("totalPrice").alias("total_revenue_from_product")
        )
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .withColumnRenamed("product", "product_most_revenue")
        .withColumnRenamed("quantity_sales", "quantity_sales_product_most_revenue")
        .withColumnRenamed("total_revenue_from_product", "total_revenue_from_product_most_revenue")
    )
    return top_product_sales

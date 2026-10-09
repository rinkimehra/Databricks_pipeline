import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")


@dlt.table(
    name=f"{catalog}.gold.fact_orders"
)
def fact_orders():
    orders = spark.read.table(f"{catalog}.silver.orders_clean")
    products = spark.read.table(f"{catalog}.silver.products_clean")

    df = orders.join(products, on="product_id", how="inner")

    df = df.withColumn(
        "total_amount", F.col("quantity") * F.col("price")
    )

    df = df.select(
        F.col("order_id"),
        F.col("order_date"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("price").alias("unit_price"),
        F.col("total_amount"),
        F.col("status"),
    )

    return df

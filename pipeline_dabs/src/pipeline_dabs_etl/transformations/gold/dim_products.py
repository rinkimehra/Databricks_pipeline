import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")


@dlt.table(
    name=f"{catalog}.gold.dim_products"
)
def dim_products():
    df = spark.read.table(f"{catalog}.silver.products_clean")

    df = df.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("price")
    )

    return df

import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")


@dlt.table(
    name=f"{catalog}.gold.dim_payments"
)
def dim_payments():
    df = spark.read.table(f"{catalog}.silver.payments_clean")

    df = df.select(
        F.col("payment_id"),
        F.col("payment_date"),
        F.col("amount"),
        F.col("order_id"),
        F.col("payment_status"),
        F.col("payment_method")
    )

    return df

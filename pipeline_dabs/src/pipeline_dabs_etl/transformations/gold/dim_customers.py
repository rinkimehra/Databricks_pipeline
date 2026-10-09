import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")


@dlt.table(
    name=f"{catalog}.gold.dim_customer"
)
def dim_customers():
    df = spark.read.table(f"{catalog}.silver.customer_scd1")

    df = df.select(
        F.col("customer_id"),
        F.col("customer_name"),
        F.col("email"),
        F.col("region"),
        F.col("signup_date")
    )

    return df

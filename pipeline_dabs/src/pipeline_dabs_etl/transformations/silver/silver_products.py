import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

def products_prepare():
    df = spark.readStream.table(f"{catalog}.bronze.products_raw")

    #casting columns product_id, product_name, category and price
    df = (
        df.withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("product_name", F.col("product_name").cast("string"))
        .withColumn("category", F.col("category").cast("string"))
        .withColumn("price", F.col("price").cast("double"))
    )

    #inconsistent casing
    df = (
        df.withColumn("product_name", lower(F.col("product_name")))
        .withColumn("category", lower(F.col("category")))
    )

    #leading trailing spaces
    df = (
        df.withColumn("product_name", trim(F.col("product_name")))
        .withColumn("category", trim(F.col("category")))
    )

    #price validation - price should be positive
    df = (
        df.withColumn("price_valid",
                    when(F.col("price") > 0, "valid")
                    .otherwise("invalid"))
    )

    #handle missing values
    df = df.fillna(value="unknown", subset=["category"])
    df = df.fillna(value=0.0, subset=["price"])

    #drop metadata columns
    df = df.drop("_rescued_data")
    df = df.drop("file_name")
    df = df.drop("file_path")
    df = df.drop("file_timestamp")

    return df


def duplicate_product_ids():
    df = spark.read.table(f"{catalog}.bronze.products_raw")

    return (
        df.groupBy("product_id").count().filter(F.col("count") > 1).select("product_id")
    )


@dlt.table(
    name=f"{catalog}.silver.products_clean"
)
def products_clean():

    df = products_prepare()

    df1 = duplicate_product_ids()

    df = (
        df
        .filter(
            (F.col("price_valid") == "valid") &
            F.col("product_id").isNotNull() &
            F.col("product_name").isNotNull())
    )

    df = df.join(df1, on="product_id", how="left_anti")

    return df


@dlt.table(
    name=f"{catalog}.silver.products_rejected"
)
def products_rejected():

    df = products_prepare()

    df1 = duplicate_product_ids()

    invalid_conditions = (
        (F.col("price_valid") == "invalid") |
        F.col("product_id").isNull() |
        F.col("product_name").isNull()
    )

    invalid_records = df.filter(invalid_conditions)

    duplicate_records = df.join(df1, on="product_id", how="inner")

    return invalid_records.unionByName(duplicate_records).dropDuplicates()




    
import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

def orders_prepare():
    df = spark.readStream.table(f"{catalog}.bronze.orders_raw")

    #type casting of orders_raw table
    df = (
        df.withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("order_date", F.col("order_date").cast("date"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn("status", F.col("status").cast("string"))
        )
    
    #manage casing of columns
    df = (
        df.withColumn("status", initcap(F.col("status")))
        .withColumn("order_date", to_date(F.col("order_date"), "yyyy-MM-dd"))
    )

    #fill null quantity, date and status with default values
    df = (
        df.fillna(value=0.0, subset=["quantity"])
        .fillna(value="unknown", subset=["status"])
    )

    #trim leading and trailing spaces
    df = df.withColumn("status", trim(F.col("status")))

    #drop metadata columns
    df = df.drop("_rescued_data")
    df = df.drop("file_name")
    df = df.drop("file_path")
    df = df.drop("file_timestamp")

    return df


def order_duplicate_ids():

    df = spark.read.table(f"{catalog}.bronze.orders_raw")

    return (
        df.groupBy("order_id")
          .count()
          .filter(col("count") > 1)
          .select("order_id")
    )


@dlt.table(
    name = f"{catalog}.silver.orders_clean"
)

def orders_clean():
    df = orders_prepare()

    df1 = order_duplicate_ids()

    customers = spark.read.table(f"{catalog}.silver.customers_clean").select("customer_id").dropDuplicates()
    products = spark.read.table(f"{catalog}.silver.products_clean").select("product_id").dropDuplicates()


    df = (
        df.filter(
            F.col("order_id").isNotNull() &
            F.col("order_date").isNotNull() &
            F.col("customer_id").isNotNull() &
            F.col("product_id").isNotNull() 
        )
    )

    df = df.join(df1, on="order_id", how="leftanti")

    df = df.join(customers, on="customer_id", how="inner")
    df = df.join(products, on="product_id", how="inner")

    return df


@dlt.table(
    name = f"{catalog}.silver.orders_rejected"
)

def orders_rejected():
    df = orders_prepare()

    df1 = order_duplicate_ids()

    customers = spark.read.table(f"{catalog}.silver.customers_clean").select("customer_id").dropDuplicates()
    products = spark.read.table(f"{catalog}.silver.products_clean").select("product_id").dropDuplicates()

    invalid_condition = (
        F.col("order_id").isNull() |
        F.col("order_date").isNull() |
        F.col("customer_id").isNull() |
        F.col("product_id").isNull()
    )

    invalid_records = df.filter(invalid_condition)

    duplicate_records = df.join(
        df1,
        on="order_id",
        how="inner"
    )

    orphan_records = (
        df
        .join(customers, on="customer_id", how="leftanti")
        .join(products, on="product_id", how="leftanti")
    )


    return invalid_records.unionByName(duplicate_records).unionByName(orphan_records).dropDuplicates()









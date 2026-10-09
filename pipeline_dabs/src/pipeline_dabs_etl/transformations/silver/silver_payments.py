import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")

def payments_prepare():
    df = spark.readStream.table(f"{catalog}.bronze.payments_raw")

    #casting the columns payment_id, payment_date, amount, order_id, payment_status, payment_method
    df = df.select(
        F.col("payment_id").cast("int").alias("payment_id"),
        F.col("payment_date").cast("timestamp").alias("payment_date"),
        F.col("amount").cast("double").alias("amount"),
        F.col("order_id").cast("int").alias("order_id"),
        F.col("payment_status").cast("string").alias("payment_status"),
        F.col("payment_method").cast("string").alias("payment_method")
    )


    #casing the columns payment_status, payment_method
    df = (
        df.withColumn("payment_status", upper(F.col("payment_status")))
        .withColumn("payment_method", upper(F.col("payment_method")))
    )


    #trim the string columns
    df = (
        df.withColumn("payment_status", trim(F.col("payment_status")))
        .withColumn("payment_method", trim(F.col("payment_method")))
    )

    #fill columns with unknown
    df = (
        df.fillna("unknown", subset=["payment_status"])
        .fillna("unknown", subset=["payment_method"])
        .fillna(0.0, subset=["amount"])
    )

    #drop metadata columns
    df = df.drop("_rescued_data")
    df = df.drop("file_name")
    df = df.drop("file_path")
    df = df.drop("file_timestamp")

    return df



def duplicate_payment_ids():
    df = spark.read.table(f"{catalog}.bronze.payments_raw")

    df = df.groupBy("payment_id").count().filter("count > 1").select("payment_id")
    return df

def valid_order_ids():
    df = spark.read.table(f"{catalog}.silver.orders_clean")

    df = df.select("order_id")
    return df

@dlt.table(
    name = f"{catalog}.silver.payments_clean"
)

def payments_clean():
    df = payments_prepare()

    df1 = duplicate_payment_ids()

    df = (
        df.filter(
            F.col("payment_id").isNotNull()
            & F.col("payment_date").isNotNull()
            & F.col("order_id").isNotNull()
        )
    )
    df = df.join(df1, on="payment_id", how="left_anti")

    #referential integrity: keep only payments with a valid order_id
    df2 = valid_order_ids()
    df = df.join(df2, on="order_id", how="inner")

    return df


@dlt.table(
    name = f"{catalog}.silver.payment_rejected"
)

def payment_rejected():
    df = payments_prepare()

    df1 = duplicate_payment_ids()

    invalid_conditions = (
        F.col("payment_id").isNull()
        | F.col("payment_date").isNull()
        | F.col("order_id").isNull()
    )

    invalid_records = df.filter(invalid_conditions)

    duplicate_records = df.join(df1, on="payment_id", how="inner")

    #referential integrity: find payments with no matching order_id
    df2 = valid_order_ids()
    orphaned_records = df.join(df2, on="order_id", how="left_anti")

    return invalid_records.unionByName(duplicate_records).unionByName(orphaned_records).dropDuplicates()


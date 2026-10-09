import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")


def customers_prepare() :
    df = spark.readStream.table(f"{catalog}.bronze.customers_raw")

    df = (
        df.withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("customer_name", F.col("customer_name").cast("string"))
        .withColumn("email", F.col("email").cast("string"))
        .withColumn("region", F.col("region").cast("string"))
        .withColumn("signup_date", to_date(F.col("signup_date"), "yyyy-MM-dd"))
        )
    
    #inconsistent casing
    df = (
        df.withColumn("customer_name", lower(F.col("customer_name")))
        .withColumn("email", lower(F.col("email")))
        .withColumn("region", upper(F.col("region")))
        )


    #leading trailing spaces
    df = (
        df.withColumn("customer_name", trim(F.col("customer_name")))
        .withColumn("email", trim(F.col("email")))
        .withColumn("region", trim(F.col("region")))
    )

    
    #email validation if email is valid contains @ ends with .com, .in, etx without regex
    df = (
        df.withColumn("email_valid", 
                    when(
                    F.col("email").contains("@") &
                    (F.col("email").endswith(".com") | 
                    F.col("email").endswith(".in") | 
                    F.col("email").endswith(".net")
                    ), 
                    "valid")
                    .otherwise("invalid"))
        )

    #handle missing values
    df = df.fillna(value = "unknown", subset = ["region"])

    #drop metadata columns
    df = df.drop("_rescued_data")
    df = df.drop("file_name")
    df = df.drop("file_path")
    df = df.drop("file_timestamp")

    return df


def duplicate_customer_ids():
    df = spark.read.table(f"{catalog}.bronze.customers_raw")

    return (
        df.groupBy("customer_id").count().filter(F.col("count") > 1).select("customer_id")
    )



@dlt.table(
    name=f"{catalog}.silver.customers_clean"
)
def customers_clean():

    df = customers_prepare()

    df1 = duplicate_customer_ids()

    df =  (
        df
        .filter(
            (F.col("email_valid") == "valid") &
            F.col("customer_id").isNotNull() &
            F.col("customer_name").isNotNull())
    )

    df = df.join(df1, on = "customer_id", how = "left_anti")

    return df

@dlt.table(
    name=f"{catalog}.silver.customers_rejected"
)
def customers_rejected():

    df = customers_prepare()

    df1 = duplicate_customer_ids()

    invalid_conditions = (
        (F.col("email_valid") == "invalid") |
        F.col("customer_id").isNull() |
        F.col("customer_name").isNull()
    )
    
    invalid_records = df.filter(invalid_conditions)
    
    duplicate_records = df.join(df1, on = "customer_id", how = "inner")

    return invalid_records.unionByName(duplicate_records).dropDuplicates()
    
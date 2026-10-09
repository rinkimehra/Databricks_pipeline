import dlt
from pyspark.sql.functions import *


#create bronze payments table
catalog = spark.conf.get("catalog")

@dlt.table(
    name = f"{catalog}.bronze.payments_raw"
)
def payments_raw():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/dev/bronze/raw_data/Customers/checkpoints/payments_schema")
        .option("schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(f"/Volumes/{catalog}/bronze/raw_data/payments/")
    )
    return (
        df
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("file_timestamp", current_timestamp())
        )
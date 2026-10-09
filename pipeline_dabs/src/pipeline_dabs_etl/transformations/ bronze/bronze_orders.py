import dlt
from pyspark.sql.functions import *


catalog = spark.conf.get("catalog")
#create bronze orders

@dlt.table(
    name = f"{catalog}.bronze.orders_raw"
)
def orders_raw():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/dev/bronze/raw_data/Customers/checkpoints/orders_schema")
        .option("schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(f"/Volumes/{catalog}/bronze/raw_data/orders/")
    )
    return (
        df
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("file_timestamp", current_timestamp())
        )
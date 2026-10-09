import dlt
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog")
#create bronze products table

@dlt.table(
    name = f"{catalog}.bronze.products_raw"
)
def products_raw():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/dev/bronze/raw_data/Customers/checkpoints/products_schema")
        .option("schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(f"/Volumes/{catalog}/bronze/raw_data/products/")
    )
    return (
        df
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("file_timestamp", current_timestamp())
        )
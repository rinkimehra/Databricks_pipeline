import dlt
from pyspark.sql.functions import col, expr

catalog = spark.conf.get("catalog")

SOURCE_TABLE = f"{catalog}.silver.customers_clean"   

dlt.create_streaming_table(
    name=f"{catalog}.silver.customer_scd1"
)

dlt.apply_changes(
    target=f"{catalog}.silver.customer_scd1",
    source=SOURCE_TABLE,
    keys=["customer_id"],               
    sequence_by=col("signup_date"),         
    stored_as_scd_type=1,                 
)

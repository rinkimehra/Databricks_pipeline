from pyspark import pipelines as dp

#catalog parameter
catalog = spark.conf.get("catalog")


@dp.table(
    name = f"{catalog}.gold.masked_customers"
)


def masked_customers():

    return spark.sql(f"""
            select 
            customer_id,
            customer_name,
            region,
            signup_date, 
            case 
            when current_user() in (select email from {catalog}.gold.access) then email 
            else mask_email(email)
            end as email
            from {catalog}.gold.dim_customer
          """)



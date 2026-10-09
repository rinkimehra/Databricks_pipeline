from pyspark import pipelines as dp

catalog = spark.conf.get("catalog")

#create materialized view of total_revenue
@dp.materialized_view(
        name = f"{catalog}.gold.total_revenue"
)
def total_revenue():
  return spark.sql(
      f"""select sum(p.amount * o.quantity) as total_revenue 
      from {catalog}.gold.fact_orders o 
      join {catalog}.gold.dim_payments p 
      on o.order_id = p.order_id
      """)


#create total orders materialized view
@dp.materialized_view(
        name = f"{catalog}.gold.total_orders"
)
def total_orders():
  return spark.sql(
      f"""select count(*) as total_orders 
      from {catalog}.gold.fact_orders
      """)

#create total Total Quantity Sold materialized view
@dp.materialized_view(
        name = f"{catalog}.gold.total_quantity_sold"
)
def total_quantity_sold():
  return spark.sql(
      f"""select sum(quantity) as total_quantity_sold 
      from {catalog}.gold.fact_orders
      """)
  

#create total revenue by month materialized view
@dp.materialized_view(
        name = f"{catalog}.gold.total_revenue_by_month"
)
def total_revenue_by_month():
  return spark.sql(
      f"""select date_format(order_date, 'yyyy-MM') as month, 
      sum(p.amount * o.quantity) as total_revenue 
      from {catalog}.gold.fact_orders o 
      join {catalog}.gold.dim_payments p 
      on o.order_id = p.order_id
      group by month
      """)


#create total revenue by product materialized view
@dp.materialized_view(
        name = f"{catalog}.gold.total_revenue_by_product"
)
def total_revenue_by_category():
  return spark.sql(
      f"""select pr.category, 
      sum(p.amount * o.quantity) as total_revenue 
      from {catalog}.gold.dim_payments p 
      join {catalog}.gold.fact_orders o 
      on o.order_id = p.order_id
      join {catalog}.gold.dim_products pr
      on o.product_id = pr.product_id
      group by pr.category
      """)
  

#create top 10 customers materialized view
@dp.materialized_view(
        name = f"{catalog}.gold.top_10_customers"
)
def top_10_customers():
  return spark.sql(
      f"""select customer_id, 
      sum(p.amount * o.quantity) as total_revenue 
      from {catalog}.gold.fact_orders o 
      join {catalog}.gold.dim_payments p 
      on o.order_id = p.order_id
      group by customer_id
      order by total_revenue desc
      limit 10
      """)
  

#create top 10 products materialized view
@dp.materialized_view(
        name = f"{catalog}.gold.top_10_products"
)
def top_10_products():
  return spark.sql(
      f"""select pr.product_id, 
      sum(p.amount * o.quantity) as total_revenue 
      from {catalog}.gold.fact_orders o 
      join {catalog}.gold.dim_payments p 
      on o.order_id = p.order_id
      join {catalog}.gold.dim_products pr
      on o.product_id = pr.product_id
      group by pr.product_id
      order by total_revenue desc
      limit 10
      """)



#create Payment Success Rate materialized view
@dp.materialized_view(
        name = f"{catalog}.gold.payment_success_rate"
)
def payment_success_rate():
  return spark.sql(
      f"""select count(*) as total_orders, 
      sum(case when payment_status = 'Success' then 1 else 0 end) as total_success,
      round(sum(case when payment_status = 'Success' then 1 else 0 end) / count(*), 2) as success_rate
      from {catalog}.gold.dim_payments
      """
  )

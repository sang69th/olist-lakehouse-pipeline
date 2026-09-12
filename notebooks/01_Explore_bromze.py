# Databricks notebook source
# MAGIC %md
# MAGIC Connecting the AWS S3 bucket with Databricks using IAM

# COMMAND ----------

import boto3, pandas as pd, io

ACCESS_KEY = "YOUR_ACCESS_KEY_ID"
SECRET_KEY = "YOUR_SECRET_ACCESS_KEY"
bucket = "olist-lakehouse-sangeeeeth"   # <-- your exact bucket name

s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="eu-west-1",          # Ireland
)

obj = s3.get_object(Bucket=bucket, Key="Bronze/olist_orders_dataset.csv")
pdf = pd.read_csv(io.BytesIO(obj["Body"].read()))
df = spark.createDataFrame(pdf)
df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC import boto3, pandas as pd, io
# MAGIC
# MAGIC ACCESS_KEY = "YOUR_ACCESS_KEY_ID"
# MAGIC SECRET_KEY = "YOUR_SECRET_ACCESS_KEY"
# MAGIC bucket     = "olist-lakehouse-sangeeeeth"
# MAGIC
# MAGIC s3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY,
# MAGIC                   aws_secret_access_key=SECRET_KEY, region_name="eu-west-1")
# MAGIC
# MAGIC def load(name):
# MAGIC     obj = s3.get_object(Bucket=bucket, Key=f"Bronze/{name}.csv")
# MAGIC     return spark.createDataFrame(pd.read_csv(io.BytesIO(obj["Body"].read())))
# MAGIC
# MAGIC print("load ready")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Converting strings to timestamps ( Silver level )

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

orders_clean = (df
    .withColumn("order_purchase_timestamp",      to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",             to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date",  to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date")))

orders_clean.printSchema()

# COMMAND ----------

before = orders_clean.count()
orders_clean = orders_clean.dropDuplicates(["order_id"])
after = orders_clean.count()
print("before:", before, "after:", after)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Before and after count remains same, meaning the dataset taken was clean and now it's proved.

# COMMAND ----------

# MAGIC %md
# MAGIC **Converting the CSV file to a Delta **

# COMMAND ----------



# COMMAND ----------

orders_clean.write.format("delta").mode("overwrite").saveAsTable("silver_orders")
print("silver_orders saved")


# COMMAND ----------



# COMMAND ----------

spark.sql("SELECT COUNT(*) AS orders FROM silver_orders").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Loading all the datasets 

# COMMAND ----------

import boto3, pandas as pd, io

ACCESS_KEY = "YOUR_ACCESS_KEY_ID"
SECRET_KEY = "YOUR_SECRET_ACCESS_KEY"
bucket     = "olist-lakehouse-sang69th"

s3 = boto3.client("s3", aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY, region_name="eu-west-1")

def load(name):
    obj = s3.get_object(Bucket=bucket, Key=f"Bronze/{name}.csv")
    return spark.createDataFrame(pd.read_csv(io.BytesIO(obj["Body"].read())))

print("load ready")

# COMMAND ----------



# COMMAND ----------

customers = load("olist_customers_dataset")
products  = load("olist_products_dataset")
sellers   = load("olist_sellers_dataset")
reviews   = load("olist_order_reviews_dataset")
items     = load("olist_order_items_dataset")
payments  = load("olist_order_payments_dataset")
print("loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning all the other tables into silver level

# COMMAND ----------

# MAGIC %md
# MAGIC Defining a function to convert tables into Silver level quality

# COMMAND ----------

def to_silver(df, key, table):
    d = df.dropDuplicates([key])
    d.write.format("delta").mode("overwrite").saveAsTable(table)
    print(f"{table}: {d.count()} rows")

to_silver(customers, "customer_id", "silver_customers")
to_silver(products,  "product_id",  "silver_products")
to_silver(sellers,   "seller_id",   "silver_sellers")
to_silver(reviews,   "review_id",   "silver_reviews")

items.write.format("delta").mode("overwrite").saveAsTable("silver_order_items")
payments.write.format("delta").mode("overwrite").saveAsTable("silver_payments")
print("silver layer complete")

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Star Schema - Fact table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_fact_orders AS
SELECT
  i.order_id,
  i.order_item_id,
  o.customer_id,
  i.product_id,
  i.seller_id,
  o.order_purchase_timestamp,
  o.order_status,
  i.price,
  i.freight_value,
  datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days,
  r.review_score
FROM silver_order_items i
JOIN silver_orders o    ON i.order_id = o.order_id
LEFT JOIN (SELECT order_id, MAX(review_score) AS review_score
           FROM silver_reviews GROUP BY order_id) r
       ON i.order_id = r.order_id
""")
print("gold_fact_orders built")

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE gold_dim_customers AS
SELECT customer_id, customer_unique_id, customer_city, customer_state
FROM silver_customers""")

spark.sql("""CREATE OR REPLACE TABLE gold_dim_products AS
SELECT product_id, product_category_name, product_weight_g,
       product_length_cm, product_height_cm, product_width_cm
FROM silver_products""")

spark.sql("""CREATE OR REPLACE TABLE gold_dim_sellers AS
SELECT seller_id, seller_city, seller_state
FROM silver_sellers""")

spark.sql("""CREATE OR REPLACE TABLE gold_dim_date AS
SELECT DISTINCT
  CAST(order_purchase_timestamp AS DATE) AS date_key,
  year(order_purchase_timestamp)  AS year,
  month(order_purchase_timestamp) AS month,
  day(order_purchase_timestamp)   AS day,
  date_format(order_purchase_timestamp,'EEEE') AS weekday
FROM silver_orders
WHERE order_purchase_timestamp IS NOT NULL""")

print("gold dimensions built")

# COMMAND ----------

spark.sql("""
SELECT c.customer_state,
       COUNT(*)                         AS order_items,
       ROUND(AVG(f.delivery_days),1)    AS avg_delivery_days,
       ROUND(AVG(f.review_score),2)     AS avg_review
FROM gold_fact_orders f
JOIN gold_dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY order_items DESC
""").show(10)

# COMMAND ----------

# MAGIC %pip install snowflake-connector-python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

conn = snowflake.connector.connect(
    user="SANG69TH",
    password="Kichu@10027032s",
    account="amhhofn-bp05819",
    warehouse="COMPUTE_WH",
    database="OLIST",
    schema="GOLD",
)

tables = ["gold_fact_orders","gold_dim_customers","gold_dim_products",
          "gold_dim_sellers","gold_dim_date"]

for t in tables:
    sdf = spark.table(t)
    pdf = pd.DataFrame([r.asDict() for r in sdf.collect()])   # avoids toPandas bug
    pdf.columns = [c.upper() for c in pdf.columns]
    write_pandas(conn, pdf, t.upper(), auto_create_table=True, overwrite=True)
    print("loaded", t, "->", len(pdf), "rows")

conn.close()
print("all gold tables in Snowflake")

# COMMAND ----------



# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

def to_pdf(sql):
    return pd.DataFrame([r.asDict() for r in spark.sql(sql).collect()])

k = to_pdf("""SELECT ROUND(SUM(price)) AS gmv,
                     COUNT(DISTINCT order_id) AS orders,
                     ROUND(AVG(review_score),2) AS avg_review,
                     ROUND(AVG(delivery_days),1) AS avg_delivery
              FROM gold_fact_orders""")
print("GMV (R$):", f"{k['gmv'][0]:,.0f}", "| Orders:", f"{k['orders'][0]:,.0f}",
      "| Avg review:", k['avg_review'][0], "| Avg delivery (days):", k['avg_delivery'][0])

cats = to_pdf("""SELECT p.product_category_name AS category, ROUND(SUM(f.price)) AS revenue
                 FROM gold_fact_orders f JOIN gold_dim_products p ON f.product_id=p.product_id
                 WHERE p.product_category_name IS NOT NULL
                 GROUP BY 1 ORDER BY revenue DESC LIMIT 10""")

status = to_pdf("""SELECT order_status, COUNT(DISTINCT order_id) AS orders
                   FROM gold_fact_orders GROUP BY 1 ORDER BY orders DESC""")

geo = to_pdf("""SELECT c.customer_state AS state, COUNT(*) AS order_items
                FROM gold_fact_orders f JOIN gold_dim_customers c ON f.customer_id=c.customer_id
                GROUP BY 1 ORDER BY order_items DESC LIMIT 12""")

fig, ax = plt.subplots(1, 3, figsize=(20, 5))
ax[0].barh(cats['category'][::-1], cats['revenue'][::-1]); ax[0].set_title("Top 10 categories by revenue (R$)")
ax[1].bar(status['order_status'], status['orders']); ax[1].set_title("Orders by status"); ax[1].tick_params(axis='x', rotation=45)
ax[2].bar(geo['state'], geo['order_items']); ax[2].set_title("Order items by state")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # To Translate into English

# COMMAND ----------

en = {
 'beleza_saude':'health_beauty',
 'relogios_presentes':'watches_gifts',
 'cama_mesa_banho':'bed_bath_table',
 'esporte_lazer':'sports_leisure',
 'informatica_acessorios':'computers_accessories',
 'moveis_decoracao':'furniture_decor',
 'cool_stuff':'cool_stuff',
 'utilidades_domesticas':'housewares',
 'automotivo':'auto',
 'ferramentas_jardim':'garden_tools',
}
cats['category'] = cats['category'].replace(en)

fig, ax = plt.subplots(1, 3, figsize=(20, 5))
ax[0].barh(cats['category'][::-1], cats['revenue'][::-1]); ax[0].set_title("Top 10 categories by revenue (R$)")
ax[1].bar(status['order_status'], status['orders']); ax[1].set_title("Orders by status"); ax[1].tick_params(axis='x', rotation=45)
ax[2].bar(geo['state'], geo['order_items']); ax[2].set_title("Order items by customer state")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
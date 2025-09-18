# Databricks notebook source
spark


import os

# COMMAND ----------

service_credential = os.environ.get("DATABRICKS_AAD_CLIENT_SECRET", "")
storage_account = os.environ.get("STORAGE_ACCOUNT_NAME", "")
application_id = os.environ.get("DATABRICKS_AAD_CLIENT_ID", "")
directory_id = os.environ.get("AZURE_TENANT_ID", "")

if not all([service_credential, storage_account, application_id, directory_id]):
    raise ValueError("Missing required environment variables for Azure AD/Storage auth. Set DATABRICKS_AAD_CLIENT_SECRET, STORAGE_ACCOUNT_NAME, DATABRICKS_AAD_CLIENT_ID, AZURE_TENANT_ID.")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

bronze_base_path = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/bronze"
customers_dataset_file_path = f"{bronze_base_path}/olist_customers_dataset.csv"
geolocation_dataset_file_path = f"{bronze_base_path}/olist_geolocation_dataset.csv"
order_items_dataset_file_path = f"{bronze_base_path}/olist_order_items_dataset.csv"
order_reviews_dataset_file_path = f"{bronze_base_path}/olist_order_reviews_dataset.csv"
orders_dataset_file_path = f"{bronze_base_path}/olist_orders_dataset.csv"
products_dataset_path = f"{bronze_base_path}/olist_products_dataset.csv"
sellers_dataset_path = f"{bronze_base_path}/olist_sellers_dataset.csv"
order_payments_dataset_path = f"{bronze_base_path}/olist_order_payments_dataset.csv"

customers_dataset_df = spark.read.option("header", "true").option("inferSchema", "true").csv(customers_dataset_file_path)
geolocation_dataset_df = spark.read.option("header", "true").option("inferSchema", "true").csv(geolocation_dataset_file_path)
order_items_dataset_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_items_dataset_file_path)
order_reviews_dataset_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_reviews_dataset_file_path)
orders_dataset_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_dataset_file_path)
products_dataset_df = spark.read.option("header", "true").option("inferSchema", "true").csv(products_dataset_path)
sellers_dataset_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sellers_dataset_path)
order_payments_dataset_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_payments_dataset_path)



# COMMAND ----------

import pandas as pd
from pymongo import MongoClient

table_name = "product_categories"
hostname = os.environ.get("MONGO_HOST", "")
database = os.environ.get("MONGO_DB", "")
port = os.environ.get("MONGO_PORT", "")
username = os.environ.get("MONGO_USER", "")
password = os.environ.get("MONGO_PASSWORD", "")

if not all([hostname, database, port, username, password]):
    raise ValueError("Missing MongoDB environment variables. Set MONGO_HOST, MONGO_DB, MONGO_PORT, MONGO_USER, MONGO_PASSWORD.")

uri = f"mongodb://{username}:{password}@{hostname}:{port}/{database}"
client = MongoClient(uri)
collection = client[database][table_name]
pandas_df = pd.DataFrame([dict(doc, _id=str(doc['_id'])) for doc in collection.find()])

spark_df = spark.createDataFrame(pandas_df)
product_category_name_df = spark_df.select("product_category_name", "product_category_name_english")



# COMMAND ----------

# MAGIC %md
# MAGIC ##Customer_dataset Cleaning
# MAGIC

# COMMAND ----------


display(customers_dataset_df.head(10))
customers_dataset_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, trim, length

string_cols = [f.name for f in customers_dataset_df.schema.fields if f.dataType.simpleString() == 'string']

for column in string_cols:
    customers_dataset_df = customers_dataset_df.withColumn(column, trim(col(column)))

customers_dataset_df = customers_dataset_df.withColumn("customer_id", col("customer_id").cast('string'))\
                        .withColumn("customer_unique_id", col("customer_unique_id").cast('string'))\
                        .withColumn("customer_zip_code_prefix",col("customer_zip_code_prefix").cast("int"))\
                        .withColumn("customer_city",col("customer_city").cast("string"))\
                        .withColumn("customer_state",col("customer_state").cast("string"))    
customers_dataset_df = customers_dataset_df.dropDuplicates()
customers_dataset_df = customers_dataset_df.dropna(subset=["customer_id", "customer_unique_id"])
customers_dataset_df = customers_dataset_df.filter(length(col("customer_zip_code_prefix")) == 5)

display(customers_dataset_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##geolocation_dataset_df Cleaning

# COMMAND ----------

display(geolocation_dataset_df.tail(20))
customers_dataset_df.printSchema() 

# COMMAND ----------

from pyspark.sql.functions import col, trim, length

string_cols = [f.name for f in geolocation_dataset_df.schema.fields if f.dataType.simpleString() == "string" ]

for column in string_cols:
    geolocation_dataset_df = geolocation_dataset_df.withColumn(column, trim(col(column)))

geolocation_dataset_df = geolocation_dataset_df.dropDuplicates()
geolocation_dataset_df = geolocation_dataset_df.dropna()
geolocation_dataset_df = geolocation_dataset_df.filter(length(col("geolocation_zip_code_prefix")) > 5)

geolocation_dataset_df = geolocation_dataset_df.withColumn("geolocation_zip_code_prefix",col("geolocation_zip_code_prefix").cast("int"))\
    .withColumn("geolocation_lat",col("geolocation_lat").cast("double"))\
    .withColumn("geolocation_lng",col("geolocation_lng").cast("double"))\
    .withColumn("geolocation_city",col("geolocation_city").cast("string"))\
    .withColumn("geolocation_state",col("geolocation_state").cast("string"))

display(geolocation_dataset_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## order_items_dataset_df Cleaning
# MAGIC

# COMMAND ----------

display(order_items_dataset_df.tail(10))
order_items_dataset_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, length, trim

string_cols = [f.name for f in order_items_dataset_df.schema.fields if f.dataType.simpleString() == 'string']

for column in string_cols:          
    order_items_dataset_df = order_items_dataset_df.withColumn(column, trim(col(column)))

order_items_dataset_df = order_items_dataset_df.dropDuplicates()
order_items_dataset_df = order_items_dataset_df.dropna()   
                                                
order_items_dataset_df = order_items_dataset_df.withColumn("order_id", col("order_id").cast('string'))\
                        .withColumn("order_item_id", col("order_item_id").cast('int'))\
                        .withColumn("product_id", col("product_id").cast('string'))\
                        .withColumn("seller_id", col("seller_id").cast('string'))\
                        .withColumn("shipping_limit_date", col("shipping_limit_date").cast('timestamp'))\
                        .withColumn("price", col("price").cast('double'))\
                        .withColumn("freight_value", col("freight_value").cast('double'))

display(order_items_dataset_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##order_reviews_dataset_df Cleaning
# MAGIC

# COMMAND ----------

display(order_reviews_dataset_df.head(10))
order_reviews_dataset_df.printSchema()



# COMMAND ----------

from pyspark.sql.functions import col, length, trim

string_cols = [f.name for f in order_reviews_dataset_df.schema.fields if f.dataType.simpleString() == 'string']

for column in string_cols:  
    order_reviews_dataset_df = order_reviews_dataset_df.withColumn(column, trim(col(column)))

order_reviews_dataset_df = order_reviews_dataset_df.dropDuplicates()
order_reviews_dataset_df = order_reviews_dataset_df.dropna(subset=["review_id", "order_id",\
                                                "review_creation_date", "review_answer_timestamp"])
order_reviews_dataset_df = order_reviews_dataset_df.withColumn("review_id", col("review_id").cast('string'))\
                        .withColumn("order_id", col("order_id").cast('string'))\
                        .withColumn("review_score", col("review_score").cast('int'))\
                        .withColumn("review_comment_title", col("review_comment_title").cast('string'))\
                        .withColumn("review_comment_message", col("review_comment_message").cast('string'))\
                        .withColumn("review_creation_date", col("review_creation_date").cast('timestamp'))\
                        .withColumn("review_answer_timestamp", col("review_answer_timestamp").cast('timestamp'))

display(order_reviews_dataset_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##orders_dataset_df Cleaning

# COMMAND ----------

display(orders_dataset_df.head(10))
orders_dataset_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, length, trim

string_cols = [f.name for f in orders_dataset_df.schema.fields if f.dataType.simpleString() == 'string']

for column in string_cols:          
    orders_dataset_df = orders_dataset_df.withColumn(column, trim(col(column))) 

orders_dataset_df = orders_dataset_df.dropDuplicates()
orders_dataset_df = orders_dataset_df.dropna(subset=["order_id", "customer_id", "order_purchase_timestamp"])

orders_dataset_df = orders_dataset_df.withColumn("order_id", col("order_id").cast('string'))\
                        .withColumn("customer_id", col("customer_id").cast('string'))\
                        .withColumn("order_status", col("order_status").cast('string'))\
                        .withColumn("order_purchase_timestamp", col("order_purchase_timestamp").cast('timestamp'))\
                        .withColumn("order_approved_at", col("order_approved_at").cast('timestamp'))\
                        .withColumn("order_delivered_carrier_date", col("order_delivered_carrier_date").cast('timestamp'))\
                        .withColumn("order_delivered_customer_date", col("order_delivered_customer_date").cast('timestamp'))\
                        .withColumn("order_estimated_delivery_date", col("order_estimated_delivery_date").cast('timestamp'))

display(orders_dataset_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##products_dataset_df Cleaning

# COMMAND ----------

display(products_dataset_df.head(10))
products_dataset_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, length, trim

string_cols = [f.name for f in products_dataset_df.schema.fields if f.dataType.simpleString() == 'string']

for column in string_cols:            
    products_dataset_df = products_dataset_df.withColumn(column, trim(col(column))) 

products_dataset_df = products_dataset_df.dropDuplicates()
products_dataset_df = products_dataset_df.dropna(subset=["product_id", "product_category_name"])
products_dataset_df = products_dataset_df.withColumn("product_id", col("product_id").cast('string'))\
                        .withColumn("product_category_name", col("product_category_name").cast('string'))\
                        .withColumn("product_name_lenght", col("product_name_lenght").cast('int'))\
                        .withColumn("product_description_lenght", col("product_description_lenght").cast('int'))\
                        .withColumn("product_photos_qty", col("product_photos_qty").cast('int'))\
                        .withColumn("product_weight_g", col("product_weight_g").cast('int'))\
                        .withColumn("product_length_cm", col("product_length_cm").cast('int'))\
                        .withColumn("product_height_cm", col("product_height_cm").cast('int'))\
                        .withColumn("product_width_cm", col("product_width_cm").cast('int'))

display(products_dataset_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##product_category_name_df Cleaning

# COMMAND ----------

display(product_category_name_df.head(10))
product_category_name_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, length, trim

product_category_name_df = product_category_name_df.dropDuplicates()
product_category_name_df = product_category_name_df.dropna(subset=["product_category_name","product_category_name_english"])
product_category_name_df = product_category_name_df.withColumn("product_category_name", col("product_category_name").cast('string'))\
                        .withColumn("product_category_name_english", col("product_category_name_english").cast('string'))

display(product_category_name_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##sellers_dataset_df cleaning

# COMMAND ----------

display(sellers_dataset_df.head(10))
sellers_dataset_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, length, trim

string_cols = [f.name for f in sellers_dataset_df.schema.fields if f.dataType.simpleString() == "string"]

for column in string_cols:            
    sellers_dataset_df = sellers_dataset_df.withColumn(column, trim(col(column))) 

sellers_dataset_df = sellers_dataset_df.dropDuplicates()    
sellers_dataset_df = sellers_dataset_df.dropna(subset=["seller_id", "seller_zip_code_prefix"])
sellers_dataset_df = sellers_dataset_df.withColumn("seller_id", col("seller_id").cast('string'))\
                        .withColumn("seller_zip_code_prefix", col("seller_zip_code_prefix").cast('int'))\
                        .withColumn("seller_city", col("seller_city").cast('string'))\
                        .withColumn("seller_state", col("seller_state").cast('string'))
                        
display(sellers_dataset_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##order_payments_dataset_df Cleaning

# COMMAND ----------

display(order_payments_dataset_df.head(10))
order_payments_dataset_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col, length, trim

string_cols = [f.name for f in order_payments_dataset_df.schema.fields if f.dataType.simpleString() == "string"]

for column in string_cols:            
    order_payments_dataset_df = order_payments_dataset_df.withColumn(column, trim(col(column))) 

order_payments_dataset_df = order_payments_dataset_df.dropDuplicates()
order_payments_dataset_df = order_payments_dataset_df.dropna(subset=["order_id", "payment_type","payment_value\
"])
order_payments_dataset_df = order_payments_dataset_df.withColumn("order_id", col("order_id").cast('string'))\
                        .withColumn("payment_type", col("payment_type").cast('string'))\
                        .withColumn("payment_installments", col("payment_installments").cast('int'))\
                        .withColumn("payment_value", col("payment_value").cast('double'))
                        
display(order_payments_dataset_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Joining Datasets

# COMMAND ----------

def remove_duplicate_columns(df):
    columns_to_drop = []
    seen_columns = set()
    for c in df.columns:
        if c in seen_columns:
            columns_to_drop.append(c)
        else:
            seen_columns.add(c)
    df_columns = df.drop(*columns_to_drop)
    return df_columns

final_joined_df = order_reviews_dataset_df.join(orders_dataset_df, "order_id", "inner")\
    .join(order_payments_dataset_df, "order_id", "inner") \
    .join(customers_dataset_df, "customer_id", "inner") \
    .join(order_items_dataset_df, "order_id", "inner") \
    .join(sellers_dataset_df, "seller_id", "inner") \
    .join(products_dataset_df, "product_id", "inner") \
    .join(product_category_name_df, "product_category_name", "inner") \
    .join(products_dataset_df, "product_id", "inner")

final_joined_df = remove_duplicate_columns(final_joined_df)
display(final_joined_df)

# COMMAND ----------

silver_base_path = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/silver/"
final_joined_df.write.mode("overwrite").parquet(silver_base_path)
#final_joined_df.write.mode("overwrite").option("header", True).csv("abfss://olistdata@olistdatastoragesagar.dfs.core.windows.net/silver/")
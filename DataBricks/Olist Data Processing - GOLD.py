# Databricks notebook source
spark

# COMMAND ----------

import os

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

silver_base_path = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/silver/"
final_data_df = spark.read.parquet(silver_base_path)
display(final_data_df)

# COMMAND ----------

delivered_df = final_data_df.filter(final_data_df.order_status == "delivered")
shipped_df = final_data_df.filter(final_data_df.order_status == "shipped")
processing_df = final_data_df.filter(final_data_df.order_status == "processing")
canceled_df = final_data_df.filter(final_data_df.order_status == "canceled")
invoiced_df = final_data_df.filter(final_data_df.order_status == "invoiced")

# COMMAND ----------

gold_parquet_base = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/gold/parquet"
delivered_df.write.mode("overwrite").parquet(f"{gold_parquet_base}/delivered/")
shipped_df.write.mode("overwrite").parquet(f"{gold_parquet_base}/shipped")
processing_df.write.mode("overwrite").parquet(f"{gold_parquet_base}/processing")
canceled_df.write.mode("overwrite").parquet(f"{gold_parquet_base}/canceled")
invoiced_df.write.mode("overwrite").parquet(f"{gold_parquet_base}/invoiced")

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

seller_revenue_df = final_data_df.groupBy("seller_id").agg(F.sum("payment_value").alias("total_revenue"))

quantiles = seller_revenue_df.approxQuantile(
    "total_revenue", 
    [0.25, 0.5, 0.75], 
    0.01 # Relative error
)
q1, q2, q3 = quantiles 

seller_revenue_df = seller_revenue_df.withColumn(
    "revenue_quartile",
    when(col("total_revenue") <= q1, "Q1")
     .when((col("total_revenue") > q1) & (col("total_revenue") <= q2), "Q2")
     .when((col("total_revenue") > q2) & (col("total_revenue") <= q3), "Q3")
     .otherwise("Q4")
)

seller_details_df = final_data_df.select(
    "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
).dropDuplicates(["seller_id"])

final_with_revenue_df = seller_details_df.join(
    seller_revenue_df.select("seller_id", "total_revenue", "revenue_quartile"),
    on="seller_id",
    how="left"
)
display(final_with_revenue_df)

# COMMAND ----------


gold_delta_base = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/gold/delta"
(final_with_revenue_df.write
    .mode("overwrite")
    .format("delta")
    .partitionBy("revenue_quartile")
    .save(f"{gold_delta_base}/seller_revenue_quartiles"))

final_data_df.write.mode("overwrite").format("delta") \
    .partitionBy("order_status") \
    .save(f"{gold_delta_base}/partitioned_by_status")

# COMMAND ----------

from pyspark.sql import Window

# 1. Orders KPIs (FIXED + ENHANCED)
orders_kpi_df = final_data_df.groupBy("order_status").agg(
    F.count_distinct("order_id").alias("total_orders"),
    F.sum("payment_value").alias("total_revenue"),
    F.avg("payment_value").alias("avg_order_value"), # FIXED
    F.count_distinct("customer_id").alias("unique_customers"),
    F.max("order_purchase_timestamp").alias("last_order_date") # NEW
)

# 2. Customer KPIs (SAFER + RICHER)
customer_kpi_df = final_data_df.groupBy("customer_id").agg(
    F.count_distinct("order_id").alias("orders_per_customer"),
    F.sum("payment_value").alias("customer_lifetime_value"),
    F.min("order_purchase_timestamp").alias("first_order_date"), # Acquisition date
    F.max("order_purchase_timestamp").alias("last_order_date"),
    F.approx_count_distinct("seller_id").alias("sellers_used") # Engagement metric
)

# 3. Seller KPIs (ADDED PERFORMANCE)
seller_kpi_df = final_data_df.groupBy("seller_id").agg(
    F.count_distinct("order_id").alias("orders_fulfilled"),
    F.sum("payment_value").alias("seller_revenue"),
    F.avg("review_score").alias("avg_rating"), # NEW - quality metric
    F.min("order_delivered_customer_date").alias("first_delivery_date") # NEW
)

# 4. Delivery KPIs (FIXED + ENHANCED)
delivery_kpi_df = final_data_df.filter(
    F.col("order_delivered_customer_date").isNotNull() & # FIXED: Added &
    F.col("order_purchase_timestamp").isNotNull()
).withColumn(
    "delivery_days",
    F.datediff("order_delivered_customer_date", "order_purchase_timestamp")
).groupBy("order_status").agg(
    F.avg("delivery_days").alias("avg_delivery_days"),
    F.expr("percentile_approx(delivery_days, 0.5)").alias("median_delivery_days"),
    F.stddev("delivery_days").alias("delivery_stddev") # NEW: Consistency metric
)
# 5. Customer Acquisition (CRITICAL)
acquisition_df = final_data_df.groupBy(
    F.year("order_purchase_timestamp").alias("signup_year"),
    F.month("order_purchase_timestamp").alias("signup_month")
).agg(
    F.count_distinct("customer_id").alias("new_customers")
)


display(orders_kpi_df)
display(customer_kpi_df)
display(seller_kpi_df)
display(delivery_kpi_df)
display(acquisition_df)


# COMMAND ----------

# Save KPI datasets
gold_kpi_base = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/gold/kpi"
orders_kpi_df.write.mode("overwrite").format("delta").save(f"{gold_kpi_base}/orders")
customer_kpi_df.write.mode("overwrite").format("delta").save(f"{gold_kpi_base}/customers")
seller_kpi_df.write.mode("overwrite").format("delta").save(f"{gold_kpi_base}/sellers")
delivery_kpi_df.write.mode("overwrite").format("delta").save(f"{gold_kpi_base}/delivery")
acquisition_df.write.mode("overwrite").format("delta").save(f"{gold_kpi_base}/acquisition")
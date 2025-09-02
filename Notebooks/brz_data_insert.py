# Databricks notebook source
# Importing from S3

df_transactions_data = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("s3://finance-dataset-bucket/brz-dataset/transactions_data.csv")

#df_transactions_data.limit(10).show()


# COMMAND ----------


df_cards_data = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("s3://finance-dataset-bucket/brz-dataset/cards_data.csv")

#df_cards_data.limit(10).show()





# COMMAND ----------

df_users_data = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("s3://finance-dataset-bucket/brz-dataset/users_data.csv")

#df_users_data.limit(10).show()


# COMMAND ----------


df_transactions_data.write.mode("overwrite").saveAsTable("finance_transactions.transactions_data_brz")
df_cards_data.write.mode("overwrite").saveAsTable("finance_transactions.cards_data_brz")
df_users_data.write.mode("overwrite").saveAsTable("finance_transactions.users_data_brz")

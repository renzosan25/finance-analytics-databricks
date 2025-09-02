# Databricks notebook source
df_transactions_data = spark.read.table("finance_transactions.transactions_data_silver")
df_cards_data = spark.read.table("finance_transactions.cards_data_silver")
df_users_data = spark.read.table("finance_transactions.users_data_silver")

df_transactions_data.printSchema()
df_cards_data.printSchema()
df_users_data.printSchema()




# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

print(f"Filas: {df_transactions_data.count()}, Columnas: {len(df_transactions_data.columns)}")
print(f"Filas: {df_cards_data.count()}, Columnas: {len(df_cards_data.columns)}")
print(f"Filas: {df_users_data.count()}, Columnas: {len(df_users_data.columns)}")

# COMMAND ----------

#Nulls

df_transactions_data.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_transactions_data.columns]).show()

df_cards_data.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_cards_data.columns]).show()

df_users_data.select([F.count(F.when(F.col(c).isNull(),c)).alias(c) for c in df_users_data.columns]).show()

# COMMAND ----------

#years

df_transactions_data.select(F.min("date"), F.max("date")).show()
#df_transactions_data.withColumn("year", F.year("date")).groupBy("year").count().orderBy("year").show()

df_cards_data.select(F.min("acct_open_date"), F.max("acct_open_date"), F.min("expires"), F.max("expires") ).show()
#df_cards_data.withColumn("year", F.year("expires")).groupBy("year").count().orderBy("year").show()






# COMMAND ----------

#Consistencia
a=df_transactions_data.join(df_users_data, 
                          df_transactions_data.client_id == df_users_data.id, 
                          "left_anti").count()


b=df_transactions_data.join(df_cards_data, 
                          df_transactions_data.card_id == df_cards_data.id, 
                          "left_anti").count()


c=df_transactions_data.alias("t") \
    .join(df_cards_data.alias("c"), F.col("t.card_id") == F.col("c.id")) \
    .filter(F.col("t.client_id") != F.col("c.client_id")) \
    .count()

print(f"1:{a} \n2:{b} \n3:{c}")

# COMMAND ----------

df_transactions_data.groupBy("merchant_state").agg(
    F.count("*").alias("num_tx"),
    F.mean("amount").alias("avg_amount"),
    F.max("amount").alias("max_amount")
).orderBy(F.desc("max_amount")).show()



# COMMAND ----------

# transactions data - cards_data 

df_transactions_data.alias("t") \
    .join(df_cards_data.alias("c"), F.col("t.card_id")==F.col("c.id")) \
    .filter(F.col("t.client_id") != F.col("c.client_id")) \
    .count()


# COMMAND ----------

df_transactions_data.approxQuantile("amount", [0.25, 0.5, 0.75, 0.95, 0.99], 0.01)


# COMMAND ----------

df_users_data.select("gender").distinct().show()
df_cards_data.select("card_brand").distinct().show()


# COMMAND ----------

df_transactions_data.select(F.min("amount").alias("min_amount"), F.max("amount").alias("max_amount")).show()

df_cards_data.select(F.min("credit_limit").alias("credit_limit"), F.max("credit_limit").alias("credit_limit")).show()

df_users_data.select(F.min("per_capita_income").alias("min_per_capita_income"), F.max("per_capita_income").alias("max_per_capita_income"),
                     F.min("yearly_income").alias("min_yearly_income"), F.max("yearly_income").alias("max_yearly_income"),
                     F.min("total_debt").alias("min_total_debt"), F.max("total_debt").alias("max_total_debt"),
                     F.min("credit_score").alias("min_credit_score"), F.max("credit_score").alias("max_credit_score")
                     ).show()

# COMMAND ----------

df_transactions_data.filter(F.col("amount") < 0).agg(F.min("amount"), F.max("amount")).show()

df_transactions_data.filter(F.col("amount") < 0) \
    .groupBy("merchant_state").count().orderBy(F.desc("count")).show()

negative = df_transactions_data.filter(F.col("amount") < 0).count()
total = df_transactions_data.count()
print(f"Result: {(negative/total)*100:.2f}% ")

# COMMAND ----------

df_transactions_data.groupBy("use_chip").count().show()
a= df_transactions_data.select("client_id").distinct().count()

print(a)

# COMMAND ----------

df_transactions_data.filter(F.col("merchant_state") == 'Turkey').show()
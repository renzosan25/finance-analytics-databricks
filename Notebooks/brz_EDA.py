# Databricks notebook source
df_transactions_data = spark.read.table("finance_transactions.transactions_data_brz")
df_cards_data = spark.read.table("finance_transactions.cards_data_brz")
df_users_data = spark.read.table("finance_transactions.users_data_brz")

df_transactions_data.printSchema()
df_cards_data.printSchema()
df_users_data.printSchema()

# transactions_data: 
# amount string -> double, clean "$"
# zip -> string

# cards_data:
# expires string -> timestamp, adjust date format
# credit_limit string -> double/integer , clean "$"
# acct_open_date string -> timestamp, adjust date format

# users_data:
# per_capita_income string -> double/integer, clean "$"
# yearly_income string -> double/ integer, clean "$"
# total_debt string -> double/ integer, clean "$"


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Estructura

print(f"Filas: {df_transactions_data.count()}, Columnas: {len(df_transactions_data.columns)}")
print(f"Filas: {df_cards_data.count()}, Columnas: {len(df_cards_data.columns)}")
print(f"Filas: {df_users_data.count()}, Columnas: {len(df_users_data.columns)}")




# COMMAND ----------

#Nulls

df_transactions_data.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_transactions_data.columns]).show()

df_cards_data.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_cards_data.columns]).show()

df_users_data.select([F.count(F.when(F.col(c).isNull(),c)).alias(c) for c in df_users_data.columns]).show()

# COMMAND ----------

df_transactions_data.groupBy("id").count().filter("count > 1").show()
df_cards_data.groupBy("id").count().filter("count > 1").show()
df_users_data.groupBy("id").count().filter("count > 1").show()

# COMMAND ----------

#transactions_data_dates

df_transactions_data.select(F.min("date"), F.max("date")).show()

df_transactions_data.withColumn("year", F.year("date")).groupBy("year").count().orderBy("year").show()


# COMMAND ----------

df_cards_data.filter(F.col("credit_limit").like("%.%")).show() # -> Double
df_users_data.filter(F.col("per_capita_income").like("%.%")).show() # Double
df_users_data.filter(F.col("yearly_income").like("%.%")).show() #Double
df_users_data.filter(F.col("total_debt").like("%.%")).show() #Double

# COMMAND ----------

df_transactions_data.filter(F.col("amount").like("%-%")).show() # -> Negative_values
df_cards_data.filter(F.col("credit_limit").like("%-%")).show() # -> None
df_users_data.filter(F.col("per_capita_income").like("%-%")).show() # None
df_users_data.filter(F.col("yearly_income").like("%-%")).show() #None
df_users_data.filter(F.col("total_debt").like("%-%")).show() #None

# COMMAND ----------

def check_dollar(df, colname):
    total = df.count()
    total_dollar = df.filter(F.col(colname).like("$%")).count()
    print(f"Celdas con dólar: {total_dollar}/{total} registros")


check_dollar(df_transactions_data, "amount")
check_dollar(df_cards_data, "credit_limit")
check_dollar(df_users_data, "per_capita_income")
check_dollar(df_users_data, "yearly_income")
check_dollar(df_users_data, "total_debt")



# COMMAND ----------

df_transactions_data.select("errors").filter(F.col("errors").isNotNull()).show()

# COMMAND ----------

#Tamaño de cadena

df_cards_data.select(F.min(F.length(F.col("expires"))).alias("min_length"),F.max(F.length(F.col("expires"))).alias("max_length")).show()

print(df_cards_data.filter(F.length(F.col("expires")) >7).count())


df_cards_data.select(F.min(F.length(F.col("acct_open_date"))).alias("min_length"),F.max(F.length(F.col("acct_open_date"))).alias("max_length")).show()

print(df_cards_data.filter(F.length(F.col("acct_open_date")) >7).count())

#df_users_data.groupBy("id").count().filter("count > 1").show()

# COMMAND ----------

df_cards_data.groupby(("card_on_dark_web")).count().show()

# COMMAND ----------


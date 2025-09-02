# Databricks notebook source
from pyspark.sql.functions import col, coalesce, lit, when, trim, regexp_replace
from pyspark.sql.types import *

df_transactions_data_b = spark.table("finance_transactions.transactions_data_brz")
df_no_dups = df_transactions_data_b.dropDuplicates()

for col_name in df_no_dups.columns:
    df_trimmed = df_no_dups.withColumn(col_name, trim(col(col_name)))

df_clean_transactions = (
    df_trimmed
    .withColumn("date", col("date").cast(TimestampType()))
    .withColumn("client_id", col("client_id").cast(IntegerType()))
    .withColumn("card_id", col("card_id").cast(IntegerType()))
    .withColumn("amount", coalesce(regexp_replace(col("amount"), "[$,]", "").cast(DoubleType()), lit(0.0)))
    .withColumn("use_chip", col("use_chip").cast(StringType()))
    .withColumn("merchant_id", col("merchant_id").cast(IntegerType()))
    .withColumn("merchant_city", col("merchant_city").cast(StringType()))
    .withColumn("merchant_state", col("merchant_state").cast(StringType()))
    .withColumn("zip", col("zip").cast(StringType()))
    .withColumn("mcc", col("mcc").cast(IntegerType()))
    .withColumn("errors", col("errors").cast(StringType()))
    .fillna({"zip": "Unknown", "merchant_state": "Unknown", "errors": "None"})
)

#df_clean.show()

df_clean_transactions.write.mode("overwrite").saveAsTable("finance_transactions.transactions_data_silver")



# COMMAND ----------

# from pyspark.sql.functions import count, when

# df_clean.select([count(when(col(c).isNull(), c)).alias(c) for c in df_clean.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import concat, to_date


df_cards_data_b = spark.table("finance_transactions.cards_data_brz")
df_no_dups = df_cards_data_b.dropDuplicates()

for col_name in df_no_dups.columns:
    df_trimmed = df_no_dups.withColumn(col_name, trim(col(col_name)))

df_clean_cards = (
    df_trimmed
    .withColumn("client_id", col("client_id").cast(IntegerType()))
    .withColumn("card_brand", col("card_brand").cast(StringType()))
    .withColumn("card_type", col("card_type").cast(StringType()))
    .withColumn("card_number", col("card_number").cast(LongType()))

    .withColumn("expires", to_date(concat(lit("01/"), col("expires")), "dd/MM/yyyy"))
    .withColumn("cvv", col("cvv").cast(IntegerType()))

    .withColumn("has_chip", when(col("has_chip").isin("YES"), 1).when(col("has_chip").isin("NO"), 0).otherwise(None))
    .withColumn("num_cards_issued", col("num_cards_issued").cast(IntegerType()))

    .withColumn("credit_limit", (regexp_replace(col("credit_limit"), "[$,]", "").cast(DoubleType())))
    .withColumn("acct_open_date", to_date(concat(lit("01/"), col("acct_open_date")), "dd/MM/yyyy"))

    .withColumn("year_pin_last_changed", col("year_pin_last_changed").cast(IntegerType()))
    .withColumn("card_on_dark_web", when(col("card_on_dark_web").isin("Yes"), 1).when(col("card_on_dark_web").isin("No"), 0).otherwise(None))

)

#df_clean_cards.show()
df_clean_cards.write.mode("overwrite").saveAsTable("finance_transactions.cards_data_silver")

# COMMAND ----------

df_users_data_b = spark.table("finance_transactions.users_data_brz")
df_no_dups = df_users_data_b.dropDuplicates()

for col_name in df_no_dups.columns:
    df_trimmed = df_no_dups.withColumn(col_name, trim(col(col_name)))

df_clean_users = (
    df_trimmed
    .withColumn("current_age", col("current_age").cast(IntegerType()))
    .withColumn("retirement_age", col("retirement_age").cast(IntegerType()))
    .withColumn("birth_year", col("birth_year").cast(IntegerType()))
    .withColumn("birth_month", col("birth_month").cast(IntegerType()))
    .withColumn("gender", col("gender").cast(StringType()))
    .withColumn("address", col("address").cast(StringType()))
    .withColumn("latitude", col("latitude").cast(DoubleType()))
    .withColumn("longitude", col("longitude").cast(DoubleType()))
    .withColumn("per_capita_income", (regexp_replace(col("per_capita_income"), "[$,]", "").cast(DoubleType())))
    .withColumn("yearly_income", (regexp_replace(col("yearly_income"), "[$,]", "").cast(DoubleType())))
    .withColumn("total_debt", (regexp_replace(col("total_debt"), "[$,]", "").cast(DoubleType())))
    .withColumn("credit_score", col("credit_score").cast(IntegerType()))
    .withColumn("num_credit_cards", col("num_credit_cards").cast(IntegerType()))
)

#df_clean_cards.show()
df_clean_users.write.mode("overwrite").saveAsTable("finance_transactions.users_data_silver")
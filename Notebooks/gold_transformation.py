# Databricks notebook source
df_transactions_data_g = spark.table("finance_transactions.transactions_data_silver")
df_cards_data_g = spark.table("finance_transactions.cards_data_silver")
df_users_data_g = spark.table("finance_transactions.users_data_silver")

#df.write.format("delta").mode("overwrite").save("s3a://your-bucket-name/cleaned-delta-data/")


# COMMAND ----------

from pyspark.sql import functions as F

df_tx_gold = (
    df_transactions_data_g
    .withColumn("spent", F.when(F.col("amount") > 0, F.col("amount")).otherwise(0))
    .withColumn("refund", F.when(F.col("amount") < 0, -F.col("amount")).otherwise(0))
    .groupBy("client_id")
    .agg(
        F.sum("spent").alias("total_spent"),
        F.sum("refund").alias("total_refunds"),
        (F.sum("spent") - F.sum("refund")).alias("net_amount"),
        F.avg(F.when(F.col("amount") > 0, F.col("amount"))).alias("avg_ticket"),
        F.max("amount").alias("max_purchase"),
        (F.sum("refund")/F.sum("spent")).alias("refund_ratio"),
        F.count("*").alias("num_tx"),
        F.countDistinct("merchant_state").alias("num_states"),
        F.count(F.when(F.col("errors").isNotNull(), 1)).alias("num_errors"),
        (F.sum(F.when(F.col("use_chip")=="Chip Transaction",1).otherwise(0))/F.count("*")).alias("pct_chip"),
        (F.sum(F.when(F.col("use_chip")=="Swipe Transaction",1).otherwise(0))/F.count("*")).alias("pct_swipe"),
        (F.sum(F.when(F.col("use_chip")=="Online Transaction",1).otherwise(0))/F.count("*")).alias("pct_online")
    )
)

df_tx_gold.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Ultima fecha como hito de referencia
cutoff_date = df_transactions_data_s.agg(F.max("date")).collect()[0][0]

df_cards_gold = (
    df_cards_data_g
    .withColumn("antiguedad_tarjeta", F.datediff(F.lit(cutoff_date), F.col("acct_open_date"))/365)
    .withColumn("expiring_6m", F.when(F.months_between(F.col("expires"), F.lit(cutoff_date)) <= 6, 1).otherwise(0))
    .groupBy("client_id")
    .agg(
        F.count("*").alias("num_cards"),
        F.sum("num_cards_issued").alias("total_cards_issued"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.sum("credit_limit").alias("total_credit_limit"),
        (F.sum(F.when(F.col("has_chip") == 1, 1).otherwise(0)) / F.count("*")).alias("pct_has_chip"),
        F.max("card_on_dark_web").alias("has_card_on_dark_web"),
        F.avg("antiguedad_tarjeta").alias("avg_card_age_years"),
        F.max("expiring_6m").alias("has_expiring_card_6m")
    )
)


df_cards_gold.show()


# COMMAND ----------

df_users_gold = (
    df_users_data_g
    .withColumn("debt_ratio", F.when(F.col("yearly_income") > 0, F.col("total_debt") / F.col("yearly_income")).otherwise(None))
    .withColumn("income_per_card", F.when(F.col("num_credit_cards") > 0, F.col("yearly_income") / F.col("num_credit_cards")).otherwise(None))
    .withColumn(
        "credit_score_band",
        F.when(F.col("credit_score") < 600, "Low")
         .when((F.col("credit_score") >= 600) & (F.col("credit_score") < 700), "Medium")
         .when(F.col("credit_score") >= 700, "High")
         .otherwise("Unknown")
    )
    .withColumn(
        "age_segment",
        F.when(F.col("current_age") < 30, "Young")
         .when((F.col("current_age") >= 30) & (F.col("current_age") < 60), "Adult")
         .when(F.col("current_age") >= 60, "Senior")
         .otherwise("Unknown")
    )
    .select(
        F.col("id").alias("client_id"),
        "current_age",
        "age_segment",
        "yearly_income",
        "per_capita_income",
        "total_debt",
        "debt_ratio",
        "num_credit_cards",
        "income_per_card",
        "credit_score",
        "credit_score_band",
        "gender"
    )
)

df_users_gold.show()


# COMMAND ----------

df_gold_final = (
    df_tx_gold
    .join(df_users_gold, "client_id", "left")
    .join(df_cards_gold, "client_id", "left")
)

df_gold_final = (
    df_gold_final
    .withColumn("fraude_flag", 
                F.when((F.col("pct_online") > 0.5) & (F.col("has_card_on_dark_web") == 1), 1).otherwise(0))
    .withColumn("premium_risk_flag", 
                F.when((F.col("yearly_income") > 100000) & (F.col("credit_score") < 600), 1).otherwise(0))
    .withColumn("spend_ratio", 
                F.when(F.col("yearly_income") > 0, F.col("net_amount") / F.col("yearly_income")).otherwise(None))
    .withColumn("credit_pressure", 
                F.when(F.col("total_credit_limit") > 0, F.col("total_debt") / F.col("total_credit_limit")).otherwise(None))
)

df_gold_final.show()


# COMMAND ----------

df_gold_final.write.mode("overwrite").saveAsTable("finance_datasets.clients_gold")


# COMMAND ----------

df_geo_gold = (
    df_transactions_data_g
    .groupBy("merchant_state")
    .agg(
        F.count("*").alias("num_tx"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount")
    )
    .filter(F.col("merchant_state").isNotNull())
)

df_geo_gold.write.mode("overwrite").saveAsTable("finance_datasets.geo_gold")

# por merchant_id
df_merchants_gold = (
    df_transactions_data_g
    .groupBy("merchant_id", "merchant_city", "merchant_state", "mcc")
    .agg(
        F.count("*").alias("num_tx"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
        F.countDistinct("client_id").alias("num_clients")
    )
    .filter(F.col("merchant_id").isNotNull())
)

df_merchants_gold.write.mode("overwrite").saveAsTable("finance_datasets.merchants_gold")






import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="gold_table"
)
def gold_table():
    acc_trans_df = dlt.read("silver_accounts_transactions")
    cust_df = dlt.read("silver_customers")
    df = acc_trans_df.join(
        cust_df,
        acc_trans_df.customer_id == cust_df.customer_id,
        "inner"
    ).drop(acc_trans_df.customer_id)
    return df

@dlt.table(
    name="gold_table_agg"
)
def gold_table_agg():
    df = dlt.read("gold_table")
    df = df.groupBy(
        "customer_id", "name", "gender", "city", "status", "income_range", "risk_segment"
    ).agg(
        countDistinct("account_id").alias("distinct_account_count"),
        count("*").alias("txn_count"),
        sum(when(col("txn_type") == "CREDIT", col("txn_amount")).otherwise(0)).alias("total_credit"),
        sum(when(col("txn_type") == "DEBIT", col("txn_amount")).otherwise(0)).alias("total_debit"),
        avg(col("txn_amount")).alias("avg_txn_amount"),
        min(col("txn_date")).alias("min_txn_date"),
        max(col("txn_date")).alias("max_txn_date"),
        countDistinct(col("txn_channel")).alias("distinct_channel_count")
    )
    return df
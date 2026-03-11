import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name='silver_accounts_transactions'
)
def silver_accounts_transactions():
    df=dlt.read_stream("bronze_accounts_transactions")
    df=df.withColumn("balance",col("balance").cast(DecimalType(10,2)))
    df=df.withColumn("txn_id",col('txn_id').cast(IntegerType()))
    df=df.withColumn("txn_date",to_date(col("txn_date"),"yyyy-MM-dd"))
    df=df.withColumn("txn_amount",col("txn_amount").cast(DecimalType(10,2)))
    string_cols=["txn_channel","txn_type","account_type"]
    for colm in string_cols:
        df=df.withColumn(colm,upper(col(colm)))
    df=df.withColumn("txn_type",when(initcap(col('txn_type'))=="Debitt","DEBIT")
                                 .when(initcap(col("txn_type"))=="Crediit","CREDIT")
                                 .otherwise(col("txn_type")))
    df=df.withColumn("channel",when(upper(col("txn_channel")).isin("ATM","BRANCH"),lit("Walk-In")).otherwise(lit("Online")))
    df=df.withColumn("month_of_transaction",month(col("txn_date"))).withColumn("year_of_transaction",year(col("txn_date"))).withColumn("day-of_transaction",dayofmonth(col('txn_date')))
    df=df.withColumn("transaction_date",when(col("txn_type")=="DEBIT",lit("out")).otherwise("in"))
                                                      
    return df


dlt.create_streaming_table("silver_accounts_transactions_scd1")
dlt.apply_changes(
        source="silver_accounts_transactions",
        target="silver_accounts_transactions_scd1",
        keys=["customer_id"],
        sequence_by="ingesttime",
        stored_as_scd_type=1
        )

dlt.create_streaming_table('silver_accounts_transaction_scd2') 
dlt.create_auto_cdc_flow(
    source="silver_accounts_transactions",
    target="silver_accounts_transaction_scd2",
    keys=["customer_id"],
    sequence_by="account_id",
    stored_as_scd_type=2
    )           





    
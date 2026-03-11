import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="bronze_accounts_transactions",
    comment="loading data from landing to bronze"
)
def bronze_accounts_transactions():
    df = dlt.readStream("loading_accounts_transactions_data")
    
    # Add missing columns
    required_columns = {
        "account_id": IntegerType(),
        "customer_id": IntegerType(),
        "account_type": StringType(),
        "balance": StringType(),
        "txn_id": StringType(),
        "txn_date": StringType(),
        "txn_amount": StringType(),
        "txn_type": StringType(),
        "txn_channel": StringType()
    }
    for col_name, col_type in required_columns.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(col_type))
    # Trim extra spaces in string columns
    string_cols = [
        "account_type", "txn_date", "txn_type",
        "txn_channel", "txn_id", "txn_amount", "balance"
    ]
    for col_name in string_cols:
        df = df.withColumn(col_name, trim(col(col_name)))
    df = df.withColumn("ingesttime", current_timestamp())
    df = df.withColumn(
        "accounts_load_id",
        date_format(current_timestamp(), "yyyyMMdd").cast(IntegerType())
    )
    #df = df.withColumn("source_file_Name", _metadata.file_path())
    # Optionally, handle inconsistent data here
    # df = df.withColumn("account_type", initcap(col("account_type")))
    # df = df.withColumn("txn_type", initcap(col("txn_type")))
    # df = df.withColumn("txn_channel", initcap(col("txn_channel")))
    # df = df.withColumn("txn_date", to_date(col("txn_date"), "yyyy-MM-dd"))
    return df
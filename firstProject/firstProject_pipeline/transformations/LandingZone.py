import dlt;
from pyspark.sql.functions import *;
from pyspark.sql.types import *;
customer_schema=StructType([
                   StructField("customer_id",StringType(),True),
                   StructField("name",StringType(),True),
                   StructField("dob",StringType(),True),
                   StructField("gender",StringType(),True),
                   StructField("city",StringType(),True),
                   StructField("join_date",StringType(),True),
                   StructField("status",StringType(),True),
                   StructField("email",StringType(),True),
                   StructField("phoneNumber",LongType(),True),
                   StructField("preferred_channel",StringType(),True),
                   StructField("occupation",StringType(),True),
                   StructField("income_range",StringType(),True),
                   StructField("risk_segment",StringType(),True)
                ])

@dlt.table(
    name="landing_customers_incremental",
    comment="landing customers data"
)
def landing_customers_incremental():
    return(
        #spark.read.table(firstproject_catalog.firstproject_schema.firstproject_volume.customers)
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        option("header","true")
        .option("cloudFiles.includeExistingFiles","true")
        .schema(customer_schema).load("/Volumes/firstproject_catalog/firstproject_schema/firstproject_volume/customers/")


    )

####################

transactions_schema=StructType([
    StructField("account_id",IntegerType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("account_type",StringType(),True),
    StructField("balance",DoubleType(),True),
    StructField("txn_id",IntegerType(),True),
    StructField("txn_date",StringType(),True),
StructField("txn_type",StringType(),True),
StructField("txn_amount",DoubleType(),True),
StructField("txn_channel",StringType(),True)
])

@dlt.table(
    name="landing_transactions_accounts",
    comment="landing transactions of the customers"
)
def landing_transactions_accounts():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.includeExistingFiles","true")
        .option("header","true")
        .schema(transactions_schema)
        .load("/Volumes/firstproject_catalog/firstproject_schema/firstproject_volume/accounts_transactions/")
    )




























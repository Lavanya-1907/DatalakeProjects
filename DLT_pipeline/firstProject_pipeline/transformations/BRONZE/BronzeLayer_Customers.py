import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="bronze_customers_cleaned",
    comment=" taking data from landing and cleaning the data in bronze layer"
)
def bronze_customers_cleaned():
    df = dlt.read_stream("landing_customers_data")
    ###Missing columns
    required_columns = {
        "customer_id": IntegerType(),
        "name": StringType(),
        "dob": StringType(),
        "gender": StringType(),
        "city": StringType(),
        "join_date": StringType(),
        "status": StringType(),
        "email": StringType(),
        "phone_number": StringType(),
        "preferred_channel": StringType(),
        "occupation": StringType(),
        "income_range": StringType(),
        "risk_segment": StringType()
    }
    for col_name, col_type in required_columns.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None).cast(col_type))

    # trimming the spaces
    df = df.withColumn("name", trim(col("name")))
    df = df.withColumn("gender", trim(col("gender")))
    df = df.withColumn("city", trim(col("city")))
    df = df.withColumn("status", trim(col("status")))
    df = df.withColumn("email", trim(col("email")))
    df = df.withColumn("phone_number", trim(col("phone_number")))
    df = df.withColumn("preferred_channel", trim(col("preferred_channel")))
    df = df.withColumn("occupation", trim(col("occupation")))
    df = df.withColumn("income_range", trim(col("income_range")))
    df = df.withColumn("risk_segment", trim(col("risk_segment")))
    df=df.withColumn("ingestion_time",current_timestamp())
    df=df.withColumn("customers_load_id",date_format(current_date(),"yyyyMMdd"))

    # string comparison checks / common value checks
   # df = df.withColumn("gender", initcap(col("gender")))
   # df = df.withColumn("status", initcap(col("status")))

    # datatype conversions
  #  df = df.withColumn("dob", to_date(col("dob"),"yyyy-MM-dd"))
  #  df = df.withColumn("join_date", to_date(col("join_date"),"yyyy-MM-dd"))

    # removing duplicates
  #  df = df.dropDuplicates(["customer_id"])

    ## REMOVING INVALID PHONE_NUMBERS
  #  df=df.withColumn("phone_number",regexp_replace(col("phone_number"),"[^0-9]",""))
  #  df=df.withColumn("is_valid_email",col("email").rlike("^[a-z0-9._]+@[a-z]+\\.com$"))


    return df
    # import dlt;
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# @dlt.table(
#     name="bronze_customers_cleaned",
#     comment=" taking data from landing and cleaning the data in bronze layer"
# )

# @dlt.expect_or_fail("valid customer_id", "customer_id is not null")
# @dlt.expect_or_drop("valid name","name is not null")
# @dlt.expect_or_drop("valid dob","dob is not null")
# @dlt.expect("valid gender","gender is not null")
# @dlt.expect_or_drop("valid city"," city is not null")
# @dlt.expect_or_drop("valid join_date","join_date is not null")
# @dlt.expect("valid status","status is not null")
# @dlt.expect("valid email","email is not null")
# @dlt.expect_or_drop("valid phone"," phone_number is not null")
# @dlt.expect_or_drop("valid preferred_channel","preferred_channel is not null")
# @dlt.expect_or_drop("valid occupation"," occupation is not null")
# def bronze_customers_cleaned():
#     df=dlt.read_stream("landing_customers_data")
#     required_columns={
#                       "customer_id":IntegerType(),
#                       "name":StringType(),
#                       "dob":StringType(),
#                       "gender":StringType(),
#                       "city":StringType(),
#                       "join_date":StringType(),
#                       "status":StringType(),
#                       "email":StringType(),
#                       "phone_number":StringType(),
#                       "preferred_channel":StringType(),
#                       "occupation":StringType(),
#                       "income_range":StringType(),
#                       "risk_segment":StringType()   
#                }
#     for col_name,col_type in required_columns.items():
#         if col_name not in df.columns:
#             df=df.withColumn(col_name,lit(None).cast(col_type))

#     ## trimming the spaces
#     df=df.withColumn("name",trim(col("name")))
#     df=df.withColumn("gender",trim(col("gender")))
#     df=df.withColumn("city",trim(col("city")))
#     df=df.withColumn("status",trim(col("status")))
#     df=df.withColumn("email",trim(col("email")))
#     df=df.withColumn("phone_number",trim(col("phone_number")))
#     df=df.withColumn("preferred_channel",trim(col("preferred_channel")))                                            
#     df=df.withColumn("occupation",trim(col("occupation")))
#     df=df.withColumn("income_range",trim(col("income_range")))
#     df=df.withColumn("risk_segment",trim(col("risk_segment")))


#     ###string comparision checks / common value checks

#     df=df.withColumn("gender",initcap(col("gender")))
#     df=df.withColumn("status",initcap(col("status")))

# ## datatype convertions
#     df=df.withColumn("dob",to_date(col("dob"),"yyyy-MM-dd"))
#     df=df.withColumn("join_date",to_date(col("join_date"),"yyyy-MM-dd"))


# # removing duplicates
#     df=df.dropDuplicates["customer_id"]

#    #################
#     return df
     
     



















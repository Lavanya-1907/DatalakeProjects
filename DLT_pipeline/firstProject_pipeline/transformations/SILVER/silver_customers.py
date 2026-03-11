import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

@dlt.table(
    name="silver_customers",
    comment="taking data from bronze to silver"
)
@dlt.expect_or_fail("valid customer_id", "customer_id is not null")
@dlt.expect("valid name","name is not null")
@dlt.expect("valid dob","dob is not null")
@dlt.expect("valid gender","gender is not null")
@dlt.expect("valid city"," city is not null")
@dlt.expect("valid join_date","join_date is not null")
@dlt.expect("valid status","status is not null")
@dlt.expect("valid email","email is not null")
@dlt.expect("valid phone"," phone_number is not null")
@dlt.expect("valid preferred_channel","preferred_channel is not null")
@dlt.expect("valid occupation"," occupation is not null")
@dlt.expect("valid income_range"," income_range is not null")
@dlt.expect("valid risk_segment"," risk_segment is not null")
def silver_customers():
    df=dlt.read_stream("bronze_customers_cleaned")
    df=df.withColumn("gender",
                     when(col("gender")=='M',"Male")
                     .when(col("gender")=='F',"Female")
                     .when(col("gender")=='X',"Cross")
                     .otherwise(col("gender"))
    )
    upper_cols=["name","email","occupation","city","income_range","risk_segment","preferred_channel"]
    for col_name in upper_cols:
        df=df.withColumn(col_name,upper(col(col_name)))


    df=df.withColumn("status",when(col('status')=="null","unknown").otherwise(col("status")))    
    df=df.withColumn("phone_number",regexp_replace(col("phone_number"),"[^0-9]",""))
    df=df.withColumn("phone_number",when(col("phone_number").rlike("^44[7][0-9]{9}$"),col("phone_number")).otherwise(None))
    # df=df.withColumn("phone_number",when(regexp_replace(col("phone_number"),'[^0-9]',"").rlike("^44[7][0-9]{9}$"),regexp_replace(col("phone_number"),'[^0-9]',"")).otherwise(None)) 

    df=df.filter(lower(col("preferred_channel")).isin("mobile","online","atm","branch"))   
    df=df.filter(lower(col("income_range")).isin("medium","high","low","veryhigh"))  
    df=df.filter(lower(col("risk_segment")).isin("low","high","medium"))


    df=df.withColumn("is_valid",when(col("customer_id").isNotNull() &
                                     col("name").isNotNull() &
                                     col("dob").isNotNull() &
                                     col("gender").isNotNull() &
                                     col("city").isNotNull() &
                                     col("join_date").isNotNull() &
                                     col("status").isNotNull() &
                                     col("email").isNotNull() &
                                     col("phone_number").isNotNull() &
                                     col("preferred_channel").isNotNull() &
                                     col("occupation").isNotNull() &
                                     col("income_range").isNotNull() &
                                     col("risk_segment").isNotNull(),True).otherwise(False))
    df=df.withColumn("email",lower(trim(col("email"))))

    df=df.withColumn("join_date",to_date(col("join_date"),"yyyy-MM-dd"))
    df=df.withColumn("dob",to_date(col("dob"),"yyyy-MM-dd"))
    df=df.withColumn("customer_age",when(col("dob").isNotNull(),floor(months_between(current_date(),col("dob"))/12)).otherwise(lit(None)))
    df=df.withColumn("tenure_days",
                     when(col("join_date").isNotNull(),floor(datediff(current_date(),col("join_date")))).otherwise(lit(None)))  
    df=df.withColumn("dob_out_of_range",(col("dob")>current_date()) | (col("dob")<lit("1900-01-01")))        
                     
  
    # window_spec=Window.partitionBy("customer_id").orderBy(col("join_date").desc())  
    # df=df.withColumn("rownumber",row_number().over(window_spec))
    # df=df.filter(col("rownumber")==1)
    # df=df.drop('rn')
    
  
    return df
# @dlt.table(name="good_data_silver")
# def good_data_silver():
#     return dlt.read("silver_customers").filter(col("is_valid")==True)  
# @dlt.table(name="bad_data_silver")
# def bad_data_silver():
#     return dlt.read("silver_customers").filter(col("is_valid")==False)




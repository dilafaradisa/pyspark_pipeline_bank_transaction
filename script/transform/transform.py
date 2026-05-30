from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, year, round, from_unixtime, make_date, year, month, dayofmonth
from pyspark.sql import functions as F
from datetime import datetime
from helper.helper import load_log_msg


def transform_marketing_campaign(spark: SparkSession, df):
    try:
        print("=" * 50)
        print("Start transforming table marketing_campaign_deposit.")
        print("=" * 50)

        current_timestamp = datetime.now()
        column_to_rename = {
            "pdays":"days_since_last_campaign",
            "previous":"previous_campaign_contacts",
            "poutcome":"previous_campaign_outcome"
        }

        df = df.withColumn('balance', F.regexp_replace(df['balance'], r"\$", " "))
        df = df.withColumn('balance', df['balance'].cast('int'))

        df = df.withColumn('duration_in_year', F.floor(df['duration'] / 365).cast('int'))

        df = df.withColumnsRenamed(column_to_rename)

        print("=" * 50)
        print("finished transforming table marketing_campaign_deposit.")
        print("=" * 50)

        log_message = spark.sparkContext \
            .parallelize([("transformation", "source", "success", "marketing_campaign_deposit", current_timestamp)]) \
            .toDF(["step", "component", "status", "table_name", "etl_date"])
        
        return df
    except Exception as e:
        print("error transforming data: ", str(e))
        
        log_message = spark.sparkContext \
            .parallelize([("transformation", "source", "failed", "marketing_campaign_deposit", current_timestamp, str(e))]) \
            .toDF(["step", "component", "status", "table_name", "etl_date", "error_msg"])
    finally:
        load_log_msg(spark=spark, log_msg=log_message)    
    

def transform_customer(spark: SparkSession, df):
    try:
        print("=" * 50)
        print("Start transforming table customers.")
        print("=" * 50)
        
        current_timestamp = datetime.now()
        column_to_rename = {
            "CustomerID":"customer_id",
            "CustomerDOB":"birth_date",
            "CustGender":"gender",
            "CustLocation":"location",
            "CustAccountBalance":"account_balance"
        }

        df = df.withColumnsRenamed(column_to_rename)
        
        df = df.withColumn("birth_date", to_date(col("birth_date"), "d/M/yy"))
        df = df.withColumn("birth_date",
                when(
                    year(col("birth_date")) > 2025,
                    make_date(
                        year(col("birth_date")) - 100,
                        month(col("birth_date")),
                        dayofmonth(col("birth_date"))
                    )
                ).otherwise(col("birth_date"))
            )
        
        df = df.withColumn('gender', F.when(col('gender')=='M', 'Male').when(col('gender')=='F', 'Female').otherwise('Other'))

        df = df.withColumn("account_balance", round(df["account_balance"].cast("float")))
        
        df = df.select(
            'customer_id', 
            'birth_date', 
            'gender', 
            'location', 
            'account_balance' 
        )
        
        log_message = spark.sparkContext \
            .parallelize([("transformation", "source", "success", "customers", current_timestamp)]) \
            .toDF(["step", "component", "status", "table_name", "etl_date"])
        
        print("=" * 50)
        print("Finished transforming table customers.")
        print("=" * 50)

        return df
        
    except Exception as e:
        print("=" * 50)
        print("error transforming data: ", str(e))
        print("=" * 50)

        log_message = spark.sparkContext \
            .parallelize([("transformation", "source", "failed", "customers", current_timestamp, str(e))]) \
            .toDF(["step", "component", "status", "table_name", "etl_date", "error_msg"])
    finally:
        load_log_msg(spark=spark,log_msg=log_message)

def transform_transaction(spark: SparkSession, df):
    try:
        print("=" * 50)
        print("Start transforming table transactions.")
        print("=" * 50)
        
        current_timestamp = datetime.now()
        column_to_rename = {
            'TransactionID':'transaction_id',
            'CustomerID':'customer_id',
            'TransactionDate':'transaction_date',
            'TransactionTime':'transaction_time',
            'TransactionAmount (INR)':'transaction_amount'
        }
        df = df.withColumnsRenamed(column_to_rename)

        df = df.withColumn("transaction_date", to_date(col("transaction_date"), "d/M/yy"))
        df = df.withColumn("transaction_date",
                when(
                    year(col("transaction_date")) > 2025,
                    make_date(
                        year(col("transaction_date")) - 100,
                        month(col("transaction_date")),
                        dayofmonth(col("transaction_date"))
                    )
                ).otherwise(col("transaction_date"))
            )

        df = df.withColumn("transaction_time",from_unixtime(col("transaction_time").cast("int"), "HH:mm:ss"))

        df = df.withColumn("transaction_amount", round(df["transaction_amount"].cast("float")))

        df = df.select(
            'transaction_id', 
            'customer_id', 
            'transaction_date', 
            'transaction_time', 
            'transaction_amount' 
        )
        print("=" * 50)
        print("Finished transforming table transactions.")
        print("=" * 50)
        log_message = spark.sparkContext \
            .parallelize([("transformation", "source", "success", "transactions", current_timestamp)]) \
            .toDF(["step", "component", "status", "table_name", "etl_date"])
        
        return df
    except Exception as e:
        print("=" * 50)
        print("Error transforming data: ", str(e))
        print("=" * 50)
        
        log_message = spark.sparkContext \
            .parallelize([("transformation", "source", "failed", "transactions", current_timestamp, str(e))]) \
            .toDF(["step", "component", "status", "table_name", "etl_date", "error_msg"])
    finally:
        load_log_msg(spark=spark, log_msg=log_message)
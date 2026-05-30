import pyspark
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from datetime import datetime
import os
from helper.helper import load_log_msg

load_dotenv()

SRC_DB_HOST=os.getenv("SRC_POSTGRES_HOST")
SRC_DB_USER=os.getenv("SRC_POSTGRES_USER")
SRC_DB_PASSWORD=os.getenv("SRC_POSTGRES_PASSWORD")
SRC_DB_PORT=os.getenv("SRC_POSTGRES_PORT")
SRC_DB_NAME=os.getenv("SRC_POSTGRES_DB") 


def extract_from_db(spark: SparkSession, table_name: str):
    """extract a table from source_db via jdbc"""
    current_timestamp = datetime.now()
    SOURCE_URL  = f"jdbc:postgresql://{SRC_DB_HOST}:5432/{SRC_DB_NAME}"
    DB_PROPERTIES = {
        "user": SRC_DB_USER,
        "password": SRC_DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    try:
        print("=" * 50)
        print(f"Extracting table: {table_name}")
        print("=" * 50)
        
        df = spark.read.jdbc(
            url=SOURCE_URL,
            table=table_name,
            properties=DB_PROPERTIES
        )
        print("=" * 50)
        print(f"Finished extracting data from table {table_name}: {df.count()} rows, {len(df.columns)} columns")
        print("=" * 50)

        log_message = spark.sparkContext \
            .parallelize([("extraction", "source", "success", table_name, current_timestamp)]) \
            .toDF(["step", "component", "status", "table_name", "etl_date"])
        
        return df
        
    except Exception as e:
        print(f"Error extracting data from table {table_name}: {e}")

        log_message = spark.sparkContext \
            .parallelize([("extraction", "source", "failed", table_name, current_timestamp, str(e))]) \
            .toDF(["step", "component", "status", "table_name", "etl_date", "error_msg"])

    finally:
        load_log_msg(spark=spark, log_msg=log_message)
        

def extract_from_csv(spark: SparkSession, path):
    """extract data from CSV file."""
    current_timestamp = datetime.now()
    
    try:
        print("=" * 50)
        print(f"Extracting CSV: {path}")
        print("=" * 50)
        
        df = spark.read.csv(
            path,
            header=True,
            inferSchema=True
        )
        
        print("=" * 50)
        print(f"Finished extracting data from {path}: {df.count()} rows, {len(df.columns)} columns")
        print("=" * 50)

        log_message = spark.sparkContext \
            .parallelize([("extraction", "source", "success", path, current_timestamp)]) \
            .toDF(["step", "component", "status", "table_name", "etl_date"])
        return df
        
    except Exception as e:
        print("=" * 50)
        print(f"Error extracting data from table {path}: {e}")
        print("=" * 50)

        log_message = spark.sparkContext \
            .parallelize([("extraction", "source", "failed", path, current_timestamp, str(e))]) \
            .toDF(["step", "component", "status", "table_name", "etl_date", "error_msg"])

    finally:
        load_log_msg(spark=spark, log_msg=log_message)
        


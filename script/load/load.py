from datetime import datetime
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from helper.helper import load_log_msg


load_dotenv()

DWH_POSTGRES_HOST=os.getenv("DWH_POSTGRES_HOST")
DWH_POSTGRES_USER=os.getenv("DWH_POSTGRES_USER")
DWH_POSTGRES_PASSWORD=os.getenv("DWH_POSTGRES_PASSWORD")
DWH_POSTGRES_PORT=os.getenv("DWH_POSTGRES_PORT")
DWH_POSTGRES_NAME=os.getenv("DWH_POSTGRES_DB") 

def load_to_dwh(spark: SparkSession, df, table_name: str):
    
    DWH_JDBC_URL = f"jdbc:postgresql://{DWH_POSTGRES_HOST}:5432/{DWH_POSTGRES_NAME}"
    current_timestamp = datetime.now()
    
    try:
        # truncate table
        connection = spark._jvm.java.sql.DriverManager.getConnection(
                DWH_JDBC_URL, DWH_POSTGRES_USER, DWH_POSTGRES_PASSWORD
            )
        statement = connection.createStatement()
        statement.executeUpdate(f"TRUNCATE TABLE {table_name} CASCADE")
        connection.close()

        print("=" * 50)
        print(f"Success truncating table: {table_name}")
        print("=" * 50)

        # start loading data
        print("=" * 50)
        print(f"start loading table: {table_name}")
        print("=" * 50)
        
        df.write.jdbc(
                url=DWH_JDBC_URL,
                table=table_name,
                mode="append",
                properties={
                    "user": DWH_POSTGRES_USER,
                    "password": DWH_POSTGRES_PASSWORD
                }
            )
        print("=" * 50)
        print(f"finished loading table: {table_name}")
        print("=" * 50)

        log_message = spark.sparkContext \
            .parallelize([("loading", "warehouse", "success", table_name, current_timestamp)]) \
            .toDF(["step", "component", "status", "table_name", "etl_date"])
        
    except Exception as e:
        print("=" * 50)
        print(f"Load process failed: {e}")
        print("=" * 50)

        log_message = spark.sparkContext\
            .parallelize([("loading", "warehouse", "failed", table_name, current_timestamp, str(e))])\
            .toDF(['step', 'component', 'status', 'table_name', 'etl_date', 'error_msg'])
        
    finally:
        load_log_msg(spark=spark, log_msg=log_message)
        
        
        
    
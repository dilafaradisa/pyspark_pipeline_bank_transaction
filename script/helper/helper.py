import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime
from pyspark.sql import SparkSession

load_dotenv()


LOG_DB_HOST=os.getenv("LOG_POSTGRES_HOST")
LOG_DB_USER=os.getenv("LOG_POSTGRES_USER")
LOG_DB_PASSWORD=os.getenv("LOG_POSTGRES_PASSWORD")
LOG_DB_PORT=os.getenv("LOG_POSTGRES_PORT")
LOG_DB_NAME=os.getenv("LOG_POSTGRES_DB")

def load_log_msg(spark: SparkSession, log_msg):

    LOG_DB_URL = f"jdbc:postgresql://{LOG_DB_HOST}:5432/{LOG_DB_NAME}"

    LOG_DB_PROPERTIES = {
    "user": LOG_DB_USER,
    "password": LOG_DB_PASSWORD,
    "driver": "org.postgresql.Driver"
    }

    table_name = "etl_log"

    log_msg.write.jdbc(url = LOG_DB_URL,
                  table = table_name,
                  mode = "append",
                  properties = LOG_DB_PROPERTIES)
    
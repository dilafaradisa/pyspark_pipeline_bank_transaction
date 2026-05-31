import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from extract.extract import extract_from_db, extract_from_csv
import json

spark = SparkSession \
        .builder \
        .appName("Data Profiling Week 6") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

df_marital_status = extract_from_db(spark = spark, table_name = "marital_status")
df_education_status = extract_from_db(spark = spark, table_name = "education_status")
df_marketing_campaign_deposit = extract_from_db(spark = spark, table_name = "marketing_campaign_deposit")
df_bank_transactions = extract_from_csv(spark = spark, path = "/home/jovyan/work/data/new_bank_transaction.csv")

def check_missing_values(df):
    result = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    return result

def check_unique_values(df):
    result = {}
    for col in df.columns:
        unique_count = df.select(F.countDistinct(F.col(col))).collect()[0][0]
        sample_vals  = [row[col] for row in df.select(col).distinct().limit(3).collect()]
        result[col] = {
            "unique_count" : unique_count,
            "sample"       : sample_vals
        }
    return result

def generate_profiling_report(dataframes: dict) -> dict:
    
    column_info ={}
    data_size = {}
    data_types = {}
    missing_values = {}
    unique_values = {}

    for name, df in dataframes.items():
        column_info[name]={"count": len(df.columns), "columns": df.columns}
        data_size[name]=df.count()
        data_types[name]=dict(df.dtypes)
        missing_values[name]=check_missing_values(df)
        unique_values[name]=check_unique_values(df)

    report = {
        "person_in_charge":"disa",
        "checking_date":datetime.now().strftime("%d/%m/%y"),
        "column_info":column_info,
        "data_size":data_size,
        "data_type":data_types,
        "missing_value":missing_values,
        "unique_value":unique_values,
    }
    print("=" * 50)
    print("generating profiling report...")
    print("=" * 50)

    return report

# dataframes = {
#     "education_status":df_education_status,
#     "marital_statustatus":df_marital_status,
#     "marketing_campaign_deposit": df_marketing_campaign_deposit,
#     "bank_transaction":df_bank_transactions,
# }
# report = generate_profiling_report(dataframes)
# # print(json.dumps(report, indent=4, default=str))

# with open(f'data_profiling.json', 'w') as file:
#     file.write(json.dumps(report, indent= 4))
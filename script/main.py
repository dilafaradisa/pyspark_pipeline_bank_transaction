from pyspark.sql import SparkSession
from extract.extract import extract_from_db, extract_from_csv
from transform.transform import transform_marketing_campaign, transform_customer, transform_transaction
from load.load import load_to_dwh

if __name__ == "__main__":
    # Inisialisasi SparkSession
    spark = SparkSession \
            .builder \
            .appName("PySpark Exercise Week 6") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()

    # Extract data 
    print("=" * 50)
    print("Starting ETL process...")
    print("=" * 50)

    df_marital_status = extract_from_db(spark = spark, table_name = "marital_status")
    df_education_status = extract_from_db(spark = spark, table_name = "education_status")
    df_marketing_campaign_deposit = extract_from_db(spark = spark, table_name = "marketing_campaign_deposit")
    df_bank_transactions = extract_from_csv(spark = spark, path = "/home/jovyan/work/data/new_bank_transaction.csv")

    # Transform data
    df_transformed_marketing_campaign = transform_marketing_campaign(spark = spark, df = df_marketing_campaign_deposit)
    df_customers = transform_customer(spark = spark, df = df_bank_transactions)
    df_transactions = transform_transaction(spark = spark, df = df_bank_transactions)

    # Load data
    load_to_dwh(spark=spark, df=df_marital_status, table_name='marital_status')
    load_to_dwh(spark=spark, df=df_education_status, table_name='education_status')
    load_to_dwh(spark=spark, df=df_transformed_marketing_campaign, table_name='marketing_campaign_deposit')
    load_to_dwh(spark=spark, df=df_customers, table_name='customers')
    load_to_dwh(spark=spark, df=df_transactions, table_name='transactions')

    print("=" * 50)
    print("ETL process finished.")
    print("=" * 50)

import argparse
import os
from datetime import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *


def ingest_index_file(spark, business_date, file_uri, output_root_uri):
    index_domain = "index"
    index_root_path = f"{output_root_uri}/{index_domain}"
    index_data_df = spark.read.csv(file_uri, header=True)
    general_bronze_pipeline(spark, index_data_df, index_domain, index_root_path)
    index_silver_pipeline(spark, business_date, index_root_path)


def general_bronze_pipeline(spark, df, domain, domain_root_file_path, partition_column='business_date'):
    (
        df
        .write
        .mode("append")
        .format("delta")
        .partitionBy(f"{partition_column}")
        .save(f"{domain_root_file_path}/bronze")
    )

    spark.sql(
        f"""
          CREATE TABLE IF NOT EXISTS {domain}_bronze
          USING DELTA
          LOCATION "{domain_root_file_path}/bronze"
        """
    )


def index_silver_pipeline(spark, business_date, index_root_path):
    (
        spark
        .read
        .format("delta")
        .load(f"{index_root_path}/bronze")
        .select(
            "index_name",
            "constituent_id",
            "ticker",
            "figi",
            "index_weight",
            "market_cap",
            col("business_date").alias("effective_business_date")
        )
        .where(col("effective_business_date") == f'{business_date}')
        .write
        .mode("append")
        .format("delta")
        .partitionBy("effective_business_date")
        .save(f"{index_root_path}/silver")
    )

    spark.sql(
        f"""
          CREATE TABLE IF NOT EXISTS index_silver
          USING DELTA
          LOCATION "{index_root_path}/silver"
        """
    )


def get_spark_session():
    return (
        SparkSession
        .builder
        .appName("OmicronLake")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def parse_arguments():
    parser = argparse.ArgumentParser(description='Pipeline bootstrap.')
    parser.add_argument('--input_file_uri', type=str,
                        help="The uri of the input file")
    parser.add_argument('--output_root_uri', type=str,
                        help="The root for the output")
    parser.add_argument('--db_name', dest="db_name", default="sirius", type=str,
                        help='The database name.')

    return parser.parse_args(sys.argv[1:])


def extract_business_date(file_path):
    file_path = file_path.replace("s3://", "")
    file_name = os.path.splitext(file_path.split('/')[-1])[0]
    file_date = file_name.split('_')[-1]
    return file_date


def canonical_business_date(file_date):
    return dt.strptime(file_date, "%Y%m%d").strftime("%Y-%m-%d")


def run_index_ingestion(args):
    spark_session = get_spark_session()
    spark_session.sql(f"USE {args.db_name}")

    business_date = canonical_business_date(extract_business_date(args.input_file_uri))

    ingest_index_file(spark_session, business_date, args.input_file_uri, args.output_root_uri)


if __name__ == '__main__':
    arguments = parse_arguments()
    run_index_ingestion(arguments)

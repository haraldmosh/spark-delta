import argparse
import os
import sys
from datetime import datetime as dt

from delta import *
from pyspark.sql import SparkSession


def gold_pipeline(spark, business_date, temp_dir):
    gold_root_path = f"{temp_dir}/index_security"

    spark.sql(
        f"""
          CREATE TABLE IF NOT EXISTS index_security(
            index_name string,
            constituent_id string,
            ticker string, 
            figi string, 
            index_weight float,
            market_cap float,
            effective_business_date string,
            security_id string,
            cusip string
          )
          USING DELTA
          LOCATION '{gold_root_path}'
          PARTITIONED BY (effective_business_date)
        """
    )

    spark.sql(
        f"""
            INSERT INTO index_security
            SELECT 
                is.index_name, 
                is.constituent_id, 
                is.ticker, 
                is.figi, 
                is.index_weight, 
                is.market_cap, 
                is.effective_business_date, 
                sls.security_id,
                sls.cusip
            FROM index_silver is
            LEFT JOIN security_latest_silver sls
            ON is.figi = sls.id_bb_global
            WHERE is.effective_business_date = '{business_date}' 
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


def parse_arguments(args):
    parser = argparse.ArgumentParser(description='Index Task.')
    parser.add_argument('--input_file_uri', type=str,
                        help="The uri of the input file")
    parser.add_argument('--output_root_uri', type=str,
                        help="The root for the output")
    parser.add_argument('--db_name', dest="db_name", default="sirius", type=str,
                        help='The database name.')

    return parser.parse_args(args)


def extract_business_date(file_path):
    file_path = file_path.replace("s3://", "")
    file_name = os.path.splitext(file_path.split('/')[-1])[0]
    file_date = file_name.split('_')[-1]
    return file_date


def canonical_business_date(file_date):
    return dt.strptime(file_date, "%Y%m%d").strftime("%Y-%m-%d")


def run_security_ingestion(args):
    spark_session = get_spark_session()
    spark_session.sql(f"USE {args.db_name}")

    business_date = canonical_business_date(extract_business_date(args.input_file_uri))

    gold_pipeline(spark_session, business_date, args.output_root_uri)


if __name__ == '__main__':
    arguments = parse_arguments(sys.argv[1:])
    run_security_ingestion(arguments)

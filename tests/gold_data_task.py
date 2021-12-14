import argparse

from delta import *
from pyspark.sql import SparkSession


def index_gold_pipeline(spark, business_date, temp_dir):
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


def parse_arguments():
    parser = argparse.ArgumentParser(description='Pipeline bootstrap.')
    parser.add_argument('--db_name', dest="db_name", default="sirius", type=str,
                        help='The database name.')
    parser.add_argument('--drop_existing_db', dest='drop_existing_db', default=False, type=bool,
                        help='If true, the database is first dropped.')

    return parser.parse_args(sys.argv[1:])


if __name__ == '__main__':
    builder = (
        SparkSession.builder
        .appName("OmicronLake")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    db_name = "sirius"
    spark.sql(f"USE {db_name}")

    business_date = '2021-12-01'
    index_gold_pipeline(spark, business_date, tmpdir)

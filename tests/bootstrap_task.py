import argparse
import sys

from pyspark.sql import SparkSession


def run_initialisation_stage(spark, db_name, recreate_db):
    if recreate_db:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")


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
    args = parse_arguments()

    run_initialisation_stage(get_spark_session(), args.db_name, args.drop_existing_db)

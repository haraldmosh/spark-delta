import argparse
import sys

from delta import *
from pyspark.sql import SparkSession


def ingest_security_file(spark, business_date, file_path, temp_dir):
    security_domain = "security"
    security_root_path = f"{temp_dir}/{security_domain}"
    security_data_df = spark.read.csv(file_path, header=True)
    general_bronze_pipeline(spark, security_data_df, security_domain, security_root_path)
    security_silver_pipeline(spark, business_date, security_root_path)


def security_silver_pipeline(spark, business_date, security_root_path):
    security_schema = f"""
        security_id string,
        security_name string,
        id_bb_global string, 
        cusip string, 
        effective_business_date string,
        end_date string,
        latest boolean
    """

    # HISTORY table

    spark.sql(
        f"""
          CREATE TABLE IF NOT EXISTS security_history_silver({security_schema})
          USING DELTA
          LOCATION '{security_root_path}/history_silver'
          PARTITIONED BY (effective_business_date)
        """
    )

    spark.sql(
        f"""
            MERGE INTO security_history_silver
            USING (
                -- This will either update existing rows or insert new rows
                SELECT 
                    security_id as merge_key, 
                    security_id,
                    security_name, 
                    id_bb_global, 
                    cusip, 
                    business_date as effective_business_date, 
                    '9999-12-31' as end_date, 
                    true as latest
                FROM security_bronze sb
                WHERE sb.business_date = '{business_date}'
                AND sb.id_bb_global IS NOT NULL
                
                UNION ALL
                
                -- This will insert the records that caused updates
                SELECT 
                    NULL as merge_key, 
                    sb.security_id,
                    sb.security_name, 
                    sb.id_bb_global, 
                    sb.cusip, 
                    sb.business_date as effective_business_date, 
                    '9999-12-31' as end_date, 
                    true as latest
                FROM security_bronze sb
                JOIN security_history_silver shs
                ON sb.security_id = shs.security_id
                AND shs.latest = true
                AND sb.business_date = '{business_date}'
                AND sb.id_bb_global IS NOT NULL
            ) AS security_bronze_bd
            ON security_history_silver.security_id = security_bronze_bd.merge_key
            WHEN MATCHED AND security_history_silver.latest = true THEN 
                UPDATE SET latest = false, end_date = security_bronze_bd.effective_business_date
            WHEN NOT MATCHED THEN INSERT *             
        """
    )

    # LATEST table

    spark.sql(
        f"""
          CREATE TABLE IF NOT EXISTS security_latest_silver({security_schema})
          USING DELTA
          LOCATION '{security_root_path}/latest_silver'
          PARTITIONED BY (effective_business_date)
        """
    )

    spark.sql(
        f"""
            MERGE INTO security_latest_silver
            USING (
                SELECT 
                    security_id,
                    security_name, 
                    id_bb_global, 
                    cusip, 
                    business_date as effective_business_date, 
                    '9999-12-31' as end_date, 
                    true as latest
                FROM security_bronze sb
                WHERE sb.business_date = '{business_date}'
                AND sb.id_bb_global IS NOT NULL
            ) AS security_bronze_bd
            ON security_latest_silver.security_id = security_bronze_bd.security_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *             
        """
    )


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
    spark_session = get_spark_session()

    db_name = "sirius"
    spark.sql(f"USE {db_name}")

    business_date = '2021-12-01'
    ingest_security_file(spark_session, business_date, "../data/security_20211201.csv", tmpdir)

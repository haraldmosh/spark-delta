from delta import *

from pyspark.sql import SparkSession

from pyspark.sql.functions import *


def test_index_security(tmpdir):
    builder = (
        SparkSession.builder
            .appName("GammaLake")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    db_name = "cyrus"
    security_table_name = "security"
    security_file_path = f"{tmpdir}/security/"
    index_table_name = "index"
    index_file_path = f"{tmpdir}/index/"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql(f"USE {db_name}")
    spark.sql(f"DROP TABLE IF EXISTS {security_table_name}")
    spark.sql(f"DROP TABLE IF EXISTS {index_table_name}")

    security_data_1_df = spark.read.csv("../data/security_20211201.csv", header=True)
    write_df_to_bronze(security_data_1_df, security_file_path)
    create_bronze_table(spark, "security_bronze", security_file_path)
    security_bronze_df = spark.read.format("delta").load(f"{security_file_path}/bronze")

    security_silver_pipeline(security_bronze_df)


def write_df_to_bronze(df, domain_root_file_path):
    (
        df
        .write
        .mode("overwrite")
        .format("delta")
        .save(f"{domain_root_file_path}/bronze")
    )


def create_bronze_table(spark, table_name, domain_root_file_path, partition_column='business_date'):
    spark.sql(
        f"""
          CREATE TABLE {table_name}
          USING DELTA
          LOCATION "{domain_root_file_path}/bronze"
          PARTITIONED BY ({partition_column})
        """
    )


def security_silver_pipeline(security_bronze_df):
    (
        security_bronze_df
            .select(
                col("security_id").cast("integer").alias("security_id"),
                "id_bb_global",
                "cusip",
                from_unixtime(unix_timestamp(col("business_date"), "yyyyddMM")).cast("date")
                    .alias("effective_business_date")
            )
    )

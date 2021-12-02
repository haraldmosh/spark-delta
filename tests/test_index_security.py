from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *


def test_index_security(tmpdir):
    builder = (
        SparkSession.builder
            .appName("GammaLake")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    db_name = "cyrus"
    security_domain = "security"
    security_root_path = f"{tmpdir}/{security_domain}"
    index_domain = "index"
    index_root_path = f"{tmpdir}/{index_domain}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql(f"USE {db_name}")
    spark.sql(f"DROP TABLE IF EXISTS {security_domain}")
    spark.sql(f"DROP TABLE IF EXISTS {index_domain}")

    security_data_1_df = spark.read.csv("../data/security_20211201.csv", header=True)
    general_bronze_pipeline(spark, security_data_1_df, security_domain, security_root_path)
    security_silver_pipeline(spark, security_root_path)

    index_data_1_df = spark.read.csv("../data/index_20211201.csv", header=True)
    general_bronze_pipeline(spark, index_data_1_df, index_domain, index_root_path)
    index_silver_pipeline(spark, index_root_path)


def general_bronze_pipeline(spark, df, domain, domain_root_file_path, partition_column='business_date'):
    (
        df
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy(f"{partition_column}")
        .save(f"{domain_root_file_path}/bronze")
    )

    spark.sql(
        f"""
          CREATE TABLE {domain}_bronze
          USING DELTA
          LOCATION "{domain_root_file_path}/bronze"
        """
    )


def security_silver_pipeline(spark, security_root_path):
    (
        spark
            .read
            .format("delta")
            .load(f"{security_root_path}/bronze")
            .select(
                "security_id",
                "id_bb_global",
                "cusip",
                from_unixtime(unix_timestamp(col("business_date"), "yyyyddMM")).cast("date")
                    .alias("effective_business_date")
            )
            .where(col("id_bb_global").isNotNull())
            .where(length("cusip") == 7)
            .partitionBy("effective_business_date")
            .save(f"{security_root_path}/silver")
    )

    spark.sql(
        f"""
          CREATE TABLE security_silver
          USING DELTA
          LOCATION "{security_root_path}/silver"
        """
    )


def index_silver_pipeline(spark, index_root_path):
    (
        spark
            .read
            .format("delta")
            .load(f"{index_root_path}/bronze")
            .select(
                "constituent_id",
                "ticker",
                "figi",
                "index_weight",
                "market_cap",
                col("business_date").alis("effective_business_date")
            )
            .partitionBy("effective_business_date")
            .save(f"{index_root_path}/silver")
    )

    spark.sql(
        f"""
          CREATE TABLE index_silver
          USING DELTA
          LOCATION "{index_root_path}/silver"
        """
    )

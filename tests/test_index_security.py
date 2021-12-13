from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *


def test_index_security(tmpdir):
    builder = (
        SparkSession.builder
            .appName("AlphaLake")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    db_name = "sirius"
    spark.sql(f"DROP DATABASE IF EXISTS {db_name}")
    spark.sql(f"CREATE DATABASE {db_name}")
    spark.sql(f"USE {db_name}")

    business_date = '2021-12-01'
    ingest_index_file(spark, business_date, "../data/index_20211201.csv", tmpdir)
    ingest_security_file(spark, business_date, "../data/security_20211201.csv", tmpdir)
    index_gold_pipeline(spark, business_date, tmpdir)

    business_date = '2021-12-02'
    ingest_index_file(spark, business_date, "../data/index_20211202.csv", tmpdir)
    index_gold_pipeline(spark, business_date, tmpdir)

    business_date = '2021-12-04'
    ingest_index_file(spark, business_date, "../data/index_20211204.csv", tmpdir)
    ingest_security_file(spark, business_date, "../data/security_20211204.csv", tmpdir)
    index_gold_pipeline(spark, business_date, tmpdir)
    data = spark.sql("select * from index_security order by effective_business_date, constituent_id").collect()


def ingest_security_file(spark, business_date, file_path, temp_dir):
    security_domain = "security"
    security_root_path = f"{temp_dir}/{security_domain}"
    security_data_df = spark.read.csv(file_path, header=True)
    general_bronze_pipeline(spark, security_data_df, security_domain, security_root_path)
    security_silver_pipeline(spark, business_date, security_root_path)


def ingest_index_file(spark, business_date, file_path, temp_dir):
    index_domain = "index"
    index_root_path = f"{temp_dir}/{index_domain}"
    index_data_df = spark.read.csv(file_path, header=True)
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

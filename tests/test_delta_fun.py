from pyspark.sql import SparkSession

from pyspark.sql.functions import expr, col


def test_run_delta():
    builder = (
        SparkSession.builder
            .appName("GammaLake")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save("/tmp/delta-table")

    df = spark.read.format("delta").load("/tmp/delta-table")
    print("Initial table")
    df.show()

    # Update even rows
    delta_table = DeltaTable.forPath(spark, "/tmp/delta-table")
    delta_table.update(
        condition=expr("id % 2 == 0 & id != 2"),
        set={"id": expr("id + 100")}
    )
    print("With even ids incremented by 100")
    delta_table.toDF().show()

    # Delete every odd value
    delta_table.delete(condition=expr("id % 2 != 0"))
    print("With odd values deleted")
    delta_table.toDF().show()

    new_data = spark.range(0, 10)

    (
        delta_table
            .alias("oldData")
            .merge(new_data.alias("newData"), "oldData.id = newData.id")
            .whenMatchedUpdate(set={"id": col("newData.id")})
            .whenNotMatchedInsert(values={"id": col("newData.id")})
            .execute()
    )
    print("After upsert")
    delta_table.toDF().show()

    # Time Travel
    df = spark.read.format("delta").option("versionAsOf", 1).load("/tmp/delta-table")
    print("Time travelling")
    df.show()


if __name__ == '__main__':
    output_location = 's3://emr-delta-lake-code'
    spark = (
        SparkSession.builder
            .appName("DeltaLake")
            .config("spark.jars", "/usr/lib/spark/jars/delta-core_2.12-1.0.0.jar")
            # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
    )

    spark.sparkContext.addPyFile("/usr/lib/spark/jars/delta-core_2.12-1.0.0.jar")

    from delta import *

    data = spark.range(0, 100)
    data.write.format("delta").mode("overwrite").save("s3://emr-delta-lake-code/delta-table")

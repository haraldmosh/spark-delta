from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: nyc_aggregations.py <s3_input_path> <s3_output_path>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession
            .builder
            .config("spark.jars", "/usr/lib/spark/jars/delta-core_2.12-1.0.0.jar")
            .appName("NYC Aggregations")
            .getOrCreate()
    )

    spark.sparkContext.addPyFile("/usr/lib/spark/jars/delta-core_2.12-1.0.0.jar")

    (
        spark
          .read
          .parquet(input_path)
          .groupBy('pulocationid', 'trip_type', 'payment_type')
          .agg(sum('fare_amount').alias('total_fare_amount'))
          .write
          .format("delta")
          .mode('overwrite')
          .save(output_path)
    )

    spark.stop()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pathlib
import os


spark = SparkSession.builder.appName(
    "Counting word occurences from a book, one more time."
).getOrCreate()


books_root_dir = os.path.join(pathlib.Path(__file__).parent.parent, "data/gutenberg_books")


results = (
    spark.read.text(books_root_dir + "/*.txt")
    .select(F.split(F.col("value"), " ").alias("line"))
    .select(F.explode(F.col("line")).alias("word"))
    .select(F.lower(F.col("word")).alias("word"))
    .select(F.regexp_extract(F.col("word"), "[a-z']+", 0).alias("word"))
    .where(F.col("word") != "")
    .groupby(F.col("word"))
    .count()
)
 
results.orderBy(F.col("count").desc()).show(10)

print("Done")
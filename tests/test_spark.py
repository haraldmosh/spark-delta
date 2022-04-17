from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("practice").getOrCreate()

people_data = [("James","","Smith","36636","M",3000),
    ("Maria","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
people_df = spark.createDataFrame(data=people_data, schema=schema)
people_df.printSchema()
people_df.show(truncate=False)


filters = [
  {"column_name": "firstname", "column_value": "Maria"},
  {"column_name": "middlename", "column_value": "Rose"}
]

spark_filter_parts = []
for f in filters:
  spark_filter_parts.append(f"{f['column_name']} = '{f['column_value']}'")
spark_filter = " AND ".join(spark_filter_parts)

people_df.filter(spark_filter).show(truncate=False)


filters = [
  {"column_name": "firstname", "column_value": "Maria"},
  {"column_name": "middlename", "column_value": "Rose"}
]

import pandas as pd
people_data_pandas = {
  'firstname': ["Maria", "Robert", "Maria", "Jen"],
  'middlename': ["Rose", "", "Anne", "Mary"]
}

people_df = pd.DataFrame(data=people_data_pandas)
filter_parts = []
for f in filters:
  filter_parts.append(f"{f['column_name']} == '{f['column_value']}'")
pandas_filter = " & ".join(filter_parts)

filtered_df = people_df.query(pandas_filter)
print(filtered_df)
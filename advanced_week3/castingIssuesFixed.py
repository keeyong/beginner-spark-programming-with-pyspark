# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark UI Test") \
    .getOrCreate()

df = spark.sql("""CREATE TEMPORARY VIEW tempView AS 
SELECT * FROM VALUES ("one", 2.3), ("two", 0.35) AS t1(col1, col2)""")
spark.sql("DESCRIBE tempView").show()

spark.sql("""SELECT * FROM tempView WHERE col2 != 0.0""").show()
spark.sql("""SELECT * FROM tempView WHERE col2 != 0.0""").explain(extended=True)

spark.sql("""SELECT * FROM tempView WHERE col2 != 0""").show()
spark.sql("""SELECT * FROM tempView WHERE col2 != 0""").explain(extended=True)

input("Waiting ...")

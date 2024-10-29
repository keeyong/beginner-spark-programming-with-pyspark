"""
Spark demo to show how to save a dataframe as Hive Partitioned table

- the dataframe has Date and value. Date is a timestamp field
- the partition should be organized by year, month, day and hour which are derived from Date
"""
from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark Partition Demo") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", True) \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.read.csv("new_orders.csv", header=True, inferSchema=True)

    df = df.withColumn("order_month", substring(df.order_date, 0, 7))
    df.show(5)
    df.write.mode("overwrite").partitionBy("order_month").saveAsTable("order")

    spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", False)
    df.write.mode("overwrite").partitionBy("order_month").coalesce(2).saveAsTable("order_v2")

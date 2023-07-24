import numpy as np
import pandas as pd
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

def show_and_count(table_path):
    df = spark.read.format("json").load(table_path)
    print("=====", table_path, "=====")
    print("count", df.count())
    print("# of partitions", df.rdd.getNumPartitions())
    print("spark.sql.files.minPartitionNum", spark.conf.get("spark.sql.files.minPartitionNum"))
    print("spark.sql.leafNodeDefaultParallelism", spark.conf.get("spark.sql.leafNodeDefaultParallelism"))
    df.groupBy(spark_partition_id()).count().show()
    print("===========================")

    return df


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ReadTesting") \
    .config("spark.sql.files.minPartitionNum", 10) \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df_norm = show_and_count("regular100")
df_gzip = show_and_count("regular100_gzip")
df_bzip2 = show_and_count("regular100_bzip2")

input("waiting ...")

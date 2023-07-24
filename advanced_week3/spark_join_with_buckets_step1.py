from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("PySpark Bucketing Example") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df1 = spark.range(1, 10 ** 6)
print(df1.rdd.getNumPartitions())
df1.write.format('parquet').mode('overwrite').bucketBy(
    100, 'id').saveAsTable('spark_bucket_table1')

df2 = spark.range(1, 10 ** 5)
print(df2.rdd.getNumPartitions())
df2.write.format('parquet').mode('overwrite').bucketBy(
    100, 'id').saveAsTable('spark_bucket_table2')

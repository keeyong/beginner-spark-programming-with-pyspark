from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Test CBO") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.cbo.enabled", True)
spark.sparkContext.setLogLevel('WARN')

spark.table("spark_bucket_table2").explain(mode="cost")

(
  spark.table("spark_bucket_table2")
  .filter(col("id") < 0)
).explain(mode="cost")

input("waiting ...")
~                    

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession \
    .builder \
    .appName("Repartition & Coalesce") \
    .config("spark.sql.adaptive.enabled", False) \
    .getOrCreate()

spark.table("order").cache().count()
spark.table("order").groupBy(spark_partition_id()).count().show()

order_10 = spark.table("order").repartition(10).cache()
order_10.count()
order_10.groupBy(spark_partition_id()).count().show()

sku_df = spark.table("order").repartition("sku")
sku_df.groupBy(spark_partition_id()).count().show()

spark.conf.set("spark.sql.adaptive.enabled", "false")
sku_df = spark.table("order").repartition("sku")
sku_df.groupBy(spark_partition_id()).count().show()

data = sku_df.groupBy(spark_partition_id()).count().rdd.glom().collect()
print(len(data))
for d in data:
    print(d)

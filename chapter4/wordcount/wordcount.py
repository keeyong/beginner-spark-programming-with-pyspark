from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("SparkSchemaDemo") \
    .config("spark.sql.adaptive.enabled", False) \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 3)

# load with schema of one column named value
df = spark.read.text("shakespeare.txt")
df.printSchema()

df_count = df.select(explode(split(df.value, " ")).alias("word")).groupBy("word").count()

df_count.show()

input("stopping ...")

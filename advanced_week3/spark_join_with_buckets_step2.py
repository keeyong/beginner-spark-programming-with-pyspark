from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("PySpark Bucketing Example") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df = spark.sql("""
    select * from spark_bucket_table1 t1
    inner join spark_bucket_table2 t2 
    on t1.id=t2.id
    """)
df.show()
df.explain(extended=True)

df = spark.sql("""
    select * from spark_bucket_table1 t1
    inner join spark_bucket_table2 t2 
    on t1.id=t2.id
    where t1.id in (100, 1000)
    """)
df.show()
df.explain(extended=True)

input("Waiting ...")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ReadTesting") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

spark.table("spark_bucket_table2").explain(extended=True)
spark.sql("DESCRIBE EXTENDED spark_bucket_table2").show(100)

spark.sql("ANALYZE TABLE spark_bucket_table2 COMPUTE STATISTICS")
spark.sql("DESCRIBE EXTENDED spark_bucket_table2").show(100)

spark.sql("ANALYZE TABLE spark_bucket_table2 COMPUTE STATISTICS FOR COLUMNS id")
spark.sql("DESCRIBE EXTENDED spark_bucket_table2 id").show(100)



input("waiting ...")

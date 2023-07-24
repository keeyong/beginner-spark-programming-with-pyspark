from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def my_func(code: str) -> str:
    # return prdCode.get(code)
    return bdData.value.get(code)


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Broadcast Variable") \
        .master("local[*]") \
        .getOrCreate()

    prdCode = spark.read.csv("data/lookup.csv").rdd.collectAsMap()

    bdData = spark.sparkContext.broadcast(prdCode)
    print("prdCode", prdCode)
    data_list = [("98312", "2021-01-01", "1200", "01"),
                 ("01056", "2021-01-02", "2345", "01"),
                 ("98312", "2021-02-03", "1200", "02"),
                 ("01056", "2021-02-04", "2345", "02"),
                 ("02845", "2021-02-05", "9812", "02")]
    df = spark.createDataFrame(data_list) \
        .toDF("code", "order_date", "price", "qty")

    spark.udf.register("my_udf", my_func, StringType())
    df.withColumn("Product", expr("my_udf(code)")) \
        .show()

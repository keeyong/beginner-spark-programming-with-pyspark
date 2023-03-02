from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *
import pandas as pd


def load_gender(spark, file_path):
    return spark.read.option("header", True).csv(file_path)


def get_gender_count(spark, df, field_to_count):
    df.createOrReplaceTempView("namegender_test")
    return spark.sql(f"SELECT {field_to_count}, COUNT(1) count FROM namegender_test GROUP BY 1")


# Define the UDF
@pandas_udf(StringType())
def upper_udf_f(s: pd.Series) -> pd.Series:
    return s.str.upper()

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark Unit Test") \
        .getOrCreate()
    upperUDF = spark.udf.register("upper_udf", upper_udf_f)

    df = spark.read.option("header", True).csv("name_gender.csv")
    df.printSchema()

    df.createOrReplaceTempView("namegender")
    spark.sql("SELECT gender, COUNT(1) count FROM namegender GROUP BY 1").show()

    df = load_gender(spark, "name_gender.csv")
    get_gender_count(spark, df, "gender").show()
    df.select(upperUDF("name").alias("NAME")).show()

    spark.stop()

# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL 저장하기") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.sql.adaptive.enabled", False) \
    .enableHiveSupport() \
    .getOrCreate()

# Redshift와 연결해서 DataFrame으로 로딩하기
url = "jdbc:redshift://learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com:5439/dev?user=guest&password=Guest1234"

df_user_session_channel = spark.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", url) \
    .option("dbtable", "raw_data.user_session_channel") \
    .load()

df_session_timestamp = spark.read \
    .format("jdbc") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .option("url", url) \
    .option("dbtable", "raw_data.session_timestamp") \
    .load()

join_expr = df_user_session_channel.sessionid == df_session_timestamp.sessionid
df_join = df_user_session_channel.join(df_session_timestamp, join_expr, "inner")
df_join.show(10)

spark.sql("DROP TABLE IF EXISTS bk_usc")
spark.sql("DROP TABLE IF EXISTS bk_st")

df_user_session_channel.write.mode("overwrite").bucketBy(3, "sessionid").saveAsTable("bk_usc")
df_session_timestamp.write.mode("overwrite").bucketBy(3, "sessionid").saveAsTable("bk_st")

df_bk_usc = spark.read.table("bk_usc")
df_bk_st = spark.read.table("bk_st")

join_expr2 = df_bk_usc.sessionid == df_bk_st.sessionid
df_join2 = df_bk_usc.join(df_bk_st, join_expr2, "inner")

df_join2.show(10)

input("Waiting ...")
